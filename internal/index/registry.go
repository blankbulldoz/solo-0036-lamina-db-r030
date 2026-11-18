package index

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

type Registry struct{ Trees map[string]*Tree }

func Build(c *catalog.Catalog, s *versions.Store) *Registry {
	r := &Registry{Trees: map[string]*Tree{}}
	for name, definition := range c.Indexes {
		tree := New()
		for key, chain := range s.Tables[definition.Relation] {
			for _, v := range chain {
				if !v.Deleted {
					tree.Add(v.Row[definition.Column], key)
				}
			}
		}
		r.Trees[name] = tree
	}
	return r
}

// Predict returns the additional candidate state that committing writes would
// retain. It is evaluated before durable publish so an under-budget commit
// cannot push index state past the instance budget after the point of no
// return. Duplicate candidates and null values cost nothing.
func (r *Registry) Predict(c *catalog.Catalog, writes []versions.Mutation) int64 {
	var total int64
	for _, m := range writes {
		if m.Deleted {
			continue
		}
		for name, i := range c.Indexes {
			if i.Relation != m.Relation {
				continue
			}
			tree := r.Trees[name]
			value := m.Row[i.Column]
			if tree != nil && tree.WouldAdd(value, m.Key) {
				total += EstimateEntry(value, m.Key)
			}
		}
	}
	return total
}

func (r *Registry) Apply(c *catalog.Catalog, writes []versions.Mutation) {
	for _, m := range writes {
		if m.Deleted {
			continue
		}
		for name, i := range c.Indexes {
			if i.Relation == m.Relation {
				r.Trees[name].Add(m.Row[i.Column], m.Key)
			}
		}
	}
}

// Bytes totals retained candidate state across all trees.
func (r *Registry) Bytes() int64 {
	var total int64
	for _, tree := range r.Trees {
		total += tree.Bytes()
	}
	return total
}
