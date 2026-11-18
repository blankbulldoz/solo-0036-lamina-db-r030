package engine

import (
	"fmt"
	"lamina/internal/journal"
	"lamina/internal/versions"
)

// Admission limits. The instance budget covers the state this process keeps
// reachable for query execution: retained version rows, secondary index
// candidates and private staged write sets. The append-only journal frames are
// deliberately excluded: they are never reclaimed in place, they are bounded by
// the 16 MiB frame limit, and counting them would make the budget monotonic and
// useless for write admission. That exclusion is a documented cost, not an
// invisible reservation.
const (
	DefaultInstanceBytes    int64 = 128 << 20
	DefaultTransactionBytes int64 = 8 << 20
	MaxTransactionKeys            = 1000
	// CostModel is reported so operators can interpret the byte numbers.
	CostModel = "deterministic retained-state estimate: per version row, index candidate and staged write (fixed slot cost plus key, column names and text payload). Journal frames and transient encode/scan buffers are excluded."
)

type Limits struct {
	InstanceBytes    int64
	TransactionBytes int64
}

func DefaultLimits() Limits {
	return Limits{InstanceBytes: DefaultInstanceBytes, TransactionBytes: DefaultTransactionBytes}
}

func (l Limits) normalized() Limits {
	if l.InstanceBytes <= 0 {
		l.InstanceBytes = DefaultInstanceBytes
	}
	if l.TransactionBytes <= 0 {
		l.TransactionBytes = DefaultTransactionBytes
	}
	if l.TransactionBytes > l.InstanceBytes {
		l.TransactionBytes = l.InstanceBytes
	}
	return l
}

// Memory reports the current admission ledger. Instance account =
// committed version bytes + index candidate bytes + private staged bytes.
type Memory struct {
	InstanceBytes      int64  `json:"instance_bytes"`
	InstanceLimit      int64  `json:"instance_limit"`
	AvailableBytes     int64  `json:"available_bytes"`
	OverBudget         bool   `json:"over_budget"`
	VersionBytes       int64  `json:"version_bytes"`
	IndexBytes         int64  `json:"index_bytes"`
	PrivateBytes       int64  `json:"private_bytes"`
	TransactionLimit   int64  `json:"transaction_limit"`
	TransactionKeyMax  int    `json:"transaction_key_limit"`
	CapacityRejections uint64 `json:"capacity_rejections_since_open"`
	CostModel          string `json:"cost_model"`
}

func (d *DB) instanceUsed() int64 { return d.versionBytes + d.indexBytes + d.privateBytes }

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Memory returns the admission ledger snapshot. It takes the read lock so
// transport responses and /stats can show it without racing a writer.
func (d *DB) Memory() Memory {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.memory()
}

func (d *DB) memory() Memory {
	used := d.instanceUsed()
	available := d.limits.InstanceBytes - used
	over := available < 0
	if available < 0 {
		available = 0
	}
	return Memory{
		InstanceBytes:      used,
		InstanceLimit:      d.limits.InstanceBytes,
		AvailableBytes:     available,
		OverBudget:         over,
		VersionBytes:       d.versionBytes,
		IndexBytes:         d.indexBytes,
		PrivateBytes:       d.privateBytes,
		TransactionLimit:   d.limits.TransactionBytes,
		TransactionKeyMax:  MaxTransactionKeys,
		CapacityRejections: d.capacityRejects,
		CostModel:          CostModel,
	}
}

// oldestSnapshot returns the lowest active snapshot and its owner count. Long
// readers appear here and explain why committed history cannot be reclaimed.
func (d *DB) oldestSnapshot() (uint64, int, bool) {
	var oldest uint64
	found := false
	count := 0
	for _, tx := range d.transactions {
		if !found || tx.Snapshot < oldest {
			oldest = tx.Snapshot
			found = true
			count = 1
		} else if tx.Snapshot == oldest {
			count++
		}
	}
	return oldest, count, found
}

func capacityHint(scope string) string {
	switch scope {
	case "transaction_bytes":
		return "replace or delete an already staged key to release private bytes; the transaction and its accepted edits stay active, and a same-key replacement only pays the difference in size"
	case "transaction_keys":
		return "delete an already staged key before staging another; replacement of an existing staged key does not consume a new key slot"
	case "instance_bytes":
		return "shrink or roll back a private write set, or commit/roll back another session; committed history is returned to the budget only when POST /vacuum actually removes it at a safe horizon (an active reader keeps it pinned)"
	case "journal_frame":
		return "reduce the final write set or raise -max-transaction-bytes only within the journal frame limit; the session and its accepted edits are preserved"
	default:
		return "reduce retained state before retrying"
	}
}

func (d *DB) capacityError(scope string, tx *Transaction, requested int64) error {
	used := d.instanceUsed()
	oldest, readers, hasOldest := d.oldestSnapshot()
	details := map[string]any{
		"scope":                 scope,
		"requested_bytes":       requested,
		"transaction_bytes":     tx.StagedBytes,
		"transaction_limit":     d.limits.TransactionBytes,
		"transaction_keys":      tx.StagedKeys,
		"transaction_key_limit": MaxTransactionKeys,
		"instance_bytes":        used,
		"instance_limit":        d.limits.InstanceBytes,
		"version_bytes":         d.versionBytes,
		"index_bytes":           d.indexBytes,
		"private_bytes":         d.privateBytes,
		"active_transactions":   len(d.transactions),
		"transaction_preserved": true,
		"hint":                  capacityHint(scope),
	}
	reclaimable := int64(-1)
	if hasOldest {
		details["oldest_snapshot"] = oldest
		details["transactions_at_oldest"] = readers
		reclaimable = d.store.PruneCandidateBytes(oldest)
	} else {
		reclaimable = d.store.PruneCandidateBytes(d.sequence)
	}
	details["reclaimable_bytes"] = reclaimable
	if scope == "instance_bytes" {
		// Report only the headroom that actually exists now. When the instance
		// is over budget, no positive replacement can be admitted until
		// history is reclaimed or another session closes, and the details must
		// not suggest otherwise.
		details["admissible_growth_bytes"] = max64(d.limits.InstanceBytes-used, 0)
		if used > d.limits.InstanceBytes {
			details["over_budget"] = true
			if reclaimable <= 0 {
				details["hint"] = "instance is already over budget and no history is reclaimable at the current safe horizon; close active readers, or stage only same-or-smaller replacements and deletes, which are always admitted"
			}
		}
	}
	var message string
	switch scope {
	case "transaction_bytes":
		message = fmt.Sprintf("transaction private write set would exceed %d bytes", d.limits.TransactionBytes)
	case "transaction_keys":
		message = fmt.Sprintf("transaction key limit %d reached", MaxTransactionKeys)
	case "instance_bytes":
		message = "instance memory budget would be exceeded"
		if tx != nil && tx.StagedBytes > 0 {
			message += fmt.Sprintf("; this transaction can still be reduced and retried with a smaller final write set")
		}
	case "journal_frame":
		message = fmt.Sprintf("commit encoding exceeds the %d byte journal frame", journal.MaxFrame)
	default:
		message = "capacity limit reached"
	}
	return faultDetails("capacity", message, details)
}

// admit applies the final-write accounting rule: only the last staged value of
// a key is retained, so a replacement is charged its size difference and a
// delete over a staged put returns bytes. Every check happens before the
// staged map or the ledger is touched, which preserves previously accepted
// edits even when this mutation is rejected.
func (d *DB) admit(tx *Transaction, table, key string, cost int64) error {
	prior := int64(0)
	exists := false
	if chain := d.store.Tables[table][key]; len(chain) > 0 {
		exists = true
	}
	if m, ok := tx.writes[table][key]; ok {
		exists = true
		prior = m.Bytes
	}
	delta := cost - prior
	keys := tx.StagedKeys
	if _, staged := tx.writes[table][key]; !staged {
		keys++
	}
	transactionBytes := tx.StagedBytes + delta
	instanceBytes := d.instanceUsed() + delta

	switch {
	case keys > MaxTransactionKeys:
		d.capacityRejects++
		return d.capacityError("transaction_keys", tx, delta)
	case !exists:
		// A first write to a key only adds private state; it is governed by the
		// transaction and instance budgets exactly as before. "Shrink" is
		// meaningful only once the key already has a retained version or a
		// staged value, so new tombstones are not mistakenly treated as
		// releases of a charge that was never paid.
		if transactionBytes > d.limits.TransactionBytes {
			d.capacityRejects++
			return d.capacityError("transaction_bytes", tx, delta)
		}
		if instanceBytes > d.limits.InstanceBytes {
			d.capacityRejects++
			return d.capacityError("instance_bytes", tx, delta)
		}
		return nil
	case delta <= 0:
		// Replacing or deleting an existing key never adds retained state, so
		// it is always admitted — including when the instance is already over
		// budget after a restart with a smaller limit. Refusing it would remove
		// the only path back under the budget.
		return nil
	case transactionBytes > d.limits.TransactionBytes:
		d.capacityRejects++
		return d.capacityError("transaction_bytes", tx, delta)
	case instanceBytes > d.limits.InstanceBytes:
		d.capacityRejects++
		return d.capacityError("instance_bytes", tx, delta)
	}
	return nil
}

func (d *DB) stage(tx *Transaction, table, key string, cost int64, value staged) {
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	prior := int64(0)
	_, exists := tx.writes[table][key]
	if exists {
		prior = tx.writes[table][key].Bytes
	}
	value.Bytes = cost
	tx.writes[table][key] = value
	if !exists {
		tx.StagedKeys++
	}
	delta := cost - prior
	tx.StagedBytes += delta
	d.privateBytes += delta
	if d.privateBytes < 0 {
		d.privateBytes = 0
	}
}

// release drops a transaction and returns its private accounting. It is used
// on rollback and on every terminal commit outcome, including conflict and
// unique failures, so a failed close never leaves bytes charged.
func (d *DB) release(tx *Transaction) {
	d.privateBytes -= tx.StagedBytes
	if d.privateBytes < 0 {
		d.privateBytes = 0
	}
	delete(d.transactions, tx.ID)
}

// chargeCommit moves the published write set from the private column into the
// committed columns. The row charge is the same version estimate, so the net
// increase is exactly the predicted index growth.
func (d *DB) chargeCommit(writes []versions.Mutation) {
	for _, m := range writes {
		d.versionBytes += versions.EstimateMutation(m)
	}
}
