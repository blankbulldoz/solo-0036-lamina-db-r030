package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
	"lamina/internal/journal"
)

func (d *DB) unique(table string, writes map[string]map[string]staged) error {
	rows := d.store.Scan(table, d.sequence)
	overlay(rows, writes, table)
	for _, i := range d.catalog.Indexes {
		if i.Relation == table && i.Unique {
			if err := checkUnique(rows, i); err != nil {
				return err
			}
		}
	}
	return nil
}
func checkUnique(rows map[string]datum.Row, i catalog.Index) error {
	values := map[string]string{}
	for key, row := range rows {
		v := row[i.Column]
		if v.Null() {
			continue
		}
		token := v.Key()
		if prior, exists := values[token]; exists && prior != key {
			return fault("conflict", "unique constraint %s violated", i.Name)
		}
		values[token] = key
	}
	return nil
}
func (d *DB) CreateRelation(r catalog.Relation) (catalog.Relation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return r, err
	}
	if err := r.Validate(); err != nil {
		return r, invalid(err)
	}
	if _, exists := d.catalog.Relations[r.Name]; exists {
		return r, fault("conflict", "relation exists")
	}
	r.Created = d.sequence + 1
	if err := d.publish(journal.Record{Kind: "relation", Relation: &r}); err != nil {
		return r, err
	}
	return r, nil
}
func (d *DB) CreateIndex(i catalog.Index) (catalog.Index, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return i, err
	}
	if err := d.catalog.CheckIndex(i); err != nil {
		return i, invalid(err)
	}
	if i.Unique {
		if err := checkUnique(d.store.Scan(i.Relation, d.sequence), i); err != nil {
			return i, err
		}
	}
	// Index candidates are retained instance state, so building an index must
	// fit the same budget as writes. Measure the exact candidate charge on a
	// throwaway tree before the durable catalog append; a build that cannot fit
	// leaves no catalog entry and no sequence, matching unique-build failure.
	probe := index.New()
	for key, chain := range d.store.Tables[i.Relation] {
		for _, v := range chain {
			if !v.Deleted {
				probe.Add(v.Row[i.Column], key)
			}
		}
	}
	if d.instanceUsed()+probe.Bytes() > d.limits.InstanceBytes {
		d.capacityRejects++
		return i, faultDetails("capacity", "index build would exceed the instance memory budget", map[string]any{
			"scope":             "index_build",
			"index_bytes":       probe.Bytes(),
			"instance_bytes":    d.instanceUsed(),
			"instance_limit":    d.limits.InstanceBytes,
			"available_bytes":   max64(d.limits.InstanceBytes-d.instanceUsed(), 0),
			"catalog_preserved": true,
			"hint":              "vacuum reclaims historical candidates; an active reader keeps them retained until it closes",
		})
	}
	i.Created = d.sequence + 1
	if err := d.publish(journal.Record{Kind: "index", Index: &i}); err != nil {
		return i, err
	}
	d.indexes = index.Build(d.catalog, d.store)
	d.indexBytes = d.indexes.Bytes()
	return i, nil
}
