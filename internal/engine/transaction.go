package engine

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"time"
)

type staged struct {
	Mutation   versions.Mutation
	InsertOnly bool
	Bytes      int64
}
type Transaction struct {
	ID          string `json:"id"`
	Snapshot    uint64 `json:"snapshot"`
	ReadOnly    bool   `json:"read_only"`
	Started     string `json:"started"`
	StagedKeys  int    `json:"staged_keys"`
	StagedBytes int64  `json:"staged_bytes"`
	writes      map[string]map[string]staged
}
type Commit struct {
	Sequence uint64 `json:"sequence"`
	Writes   int    `json:"writes"`
}

func (d *DB) Begin(readOnly bool) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	if len(d.transactions) >= 128 {
		return Transaction{}, fault("capacity", "128 active transactions reached")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Transaction{}, err
	}
	tx := &Transaction{ID: hex.EncodeToString(id[:]), Snapshot: d.sequence, ReadOnly: readOnly, Started: time.Now().UTC().Format(time.RFC3339Nano), writes: map[string]map[string]staged{}}
	d.transactions[tx.ID] = tx
	out := *tx
	out.writes = nil
	return out, nil
}
func (d *DB) tx(id string) (*Transaction, error) {
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	return tx, nil
}

// stagePut validates a put, computes its accounting charge, and admits it
// without disturbing the transaction if the charge does not fit. The returned
// error is terminal only in transport terms: the active transaction and every
// previously accepted staged edit remain usable, so the caller can shrink the
// write set and retry. A same-key replacement pays only the size difference.
func (d *DB) Put(id, table string, row datum.Row, insertOnly bool) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Transaction{}, err
	}
	if tx.ReadOnly {
		return Transaction{}, fault("conflict", "read-only transaction")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return Transaction{}, invalid(err)
	}
	if r.Created > tx.Snapshot {
		return Transaction{}, fault("conflict", "relation outside transaction catalog snapshot")
	}
	normalized, err := r.Normalize(row)
	if err != nil {
		return Transaction{}, invalid(err)
	}
	key := r.Key(normalized)
	rows := d.store.Scan(table, tx.Snapshot)
	overlay(rows, tx.writes, table)
	if insertOnly {
		if _, ok := rows[key]; ok {
			return Transaction{}, fault("conflict", "primary already exists")
		}
	}
	mutation := versions.Mutation{Relation: table, Key: key, Row: normalized}
	if err = d.admit(tx, table, key, versions.EstimateMutation(mutation)); err != nil {
		return Transaction{}, err
	}
	d.stage(tx, table, key, versions.EstimateMutation(mutation), staged{Mutation: mutation, InsertOnly: insertOnly})
	return d.view(tx), nil
}

func (d *DB) Delete(id, table string, value datum.Value) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Transaction{}, err
	}
	if tx.ReadOnly {
		return Transaction{}, fault("conflict", "read-only transaction")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return Transaction{}, invalid(err)
	}
	if r.Created > tx.Snapshot {
		return Transaction{}, fault("conflict", "relation outside transaction catalog snapshot")
	}
	col, _ := r.Column(r.Primary)
	if value.Null() || value.Kind != col.Type {
		return Transaction{}, fault("invalid", "invalid primary value")
	}
	key := value.Key()
	mutation := versions.Mutation{Relation: table, Key: key, Deleted: true}
	if err = d.admit(tx, table, key, versions.EstimateMutation(mutation)); err != nil {
		return Transaction{}, err
	}
	d.stage(tx, table, key, versions.EstimateMutation(mutation), staged{Mutation: mutation})
	return d.view(tx), nil
}

// view returns a copy of the public transaction fields. The private staged map
// is never exposed because callers must go through admission to change it.
func (d *DB) view(tx *Transaction) Transaction {
	out := *tx
	out.writes = nil
	return out
}

func (d *DB) Rollback(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	d.release(tx)
	return nil
}

// Commit validates conflicts and unique indexes, then predicts both durable
// version growth and index growth before publishing. A capacity failure at
// this point is a refusal, not a commit failure: no journal record is encoded,
// no sequence is consumed, no conflict is counted, and the transaction stays
// active with all accepted edits so the caller can shrink the write set and
// retry. Conflict, unique and zero-write outcomes remain terminal exactly as
// before.
func (d *DB) Commit(id string) (Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Commit{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Commit{}, err
	}
	writes := make([]versions.Mutation, 0)
	for table, mutations := range tx.writes {
		for key, m := range mutations {
			if d.store.Latest(table, key) > tx.Snapshot {
				d.conflicts++
				d.release(tx)
				return Commit{}, fault("conflict", "write conflict on %s", table)
			}
			writes = append(writes, m.Mutation)
		}
		if err = d.unique(table, tx.writes); err != nil {
			d.conflicts++
			d.release(tx)
			return Commit{}, err
		}
	}
	if len(writes) == 0 {
		d.release(tx)
		return Commit{Sequence: tx.Snapshot}, nil
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Relation == writes[j].Relation {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Relation < writes[j].Relation
	})
	// The private charge is replaced by the durable version charge and the
	// staged rows stop being private, so the net instance increase is the new
	// version rows plus newly retained index candidates minus the staged bytes
	// this commit releases. Predict both before the durable append so the
	// budget cannot be breached after the point of no return. Checking the net
	// increase, rather than reserving the full commit again, is what lets a
	// write set that already fits in the private column commit unchanged.
	var growth int64
	for _, m := range writes {
		growth += versions.EstimateMutation(m)
	}
	growth += d.indexes.Predict(d.catalog, writes)
	net := d.instanceUsed() - tx.StagedBytes + growth
	// Like single-key admission, a commit whose net retained state does not
	// grow (for example all-tombstone write sets) is allowed even when the
	// instance already exceeds its budget; otherwise the budget could not be
	// recovered without discarding accepted edits.
	if net > d.limits.InstanceBytes && net > d.instanceUsed() {
		d.capacityRejects++
		return Commit{}, d.capacityError("instance_bytes", tx, net-d.instanceUsed())
	}
	record := journal.Record{Kind: "commit", Transaction: id, Writes: writes}
	// The transaction byte budget normally keeps a commit far below the 16MiB
	// journal frame, but an operator can raise the budget. Check the encoded
	// frame before the durable append (which would otherwise fail after the
	// point of no return) and report it as a capacity refusal, not storage
	// uncertainty. This costs one extra encode of an already size-bounded set.
	if encoded, err := json.Marshal(record); err != nil {
		return Commit{}, fault("invalid", "commit encoding failed: %s", err)
	} else if len(encoded) > journal.MaxFrame {
		d.capacityRejects++
		return Commit{}, d.capacityError("journal_frame", tx, int64(len(encoded)))
	}
	if err = d.publish(record); err != nil {
		d.release(tx)
		return Commit{}, err
	}
	d.chargeCommit(writes)
	d.release(tx)
	d.commits++
	return Commit{Sequence: d.sequence, Writes: len(writes)}, nil
}
