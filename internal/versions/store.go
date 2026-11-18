package versions

import "lamina/internal/datum"

// The admission ledger deliberately uses a deterministic estimate rather than
// runtime heap measurements. The constants cover the map slots, slice entries
// and retained strings that a staged mutation or a committed version keeps
// alive. They are accounting units, not a promise about Go allocator rounding.
const (
	mutationOverhead int64 = 96
	rowOverhead      int64 = 48
	columnOverhead   int64 = 24
)

type Mutation struct {
	Relation string    `json:"relation"`
	Key      string    `json:"key"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}
type Version struct {
	Sequence uint64    `json:"sequence"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}
type Store struct {
	Tables map[string]map[string][]Version `json:"tables"`
}

func New() *Store { return &Store{Tables: map[string]map[string][]Version{}} }
func (s *Store) Ensure(name string) {
	if s.Tables[name] == nil {
		s.Tables[name] = map[string][]Version{}
	}
}
func (s *Store) Apply(seq uint64, writes []Mutation) {
	for _, m := range writes {
		s.Ensure(m.Relation)
		s.Tables[m.Relation][m.Key] = append(s.Tables[m.Relation][m.Key], Version{Sequence: seq, Row: datum.Clone(m.Row), Deleted: m.Deleted})
	}
}
func Visible(chain []Version, at uint64) (Version, bool) {
	lo, hi := 0, len(chain)
	for lo < hi {
		mid := (lo + hi) / 2
		if chain[mid].Sequence <= at {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return Version{}, false
	}
	v := chain[lo-1]
	return v, !v.Deleted
}
func (s *Store) Read(table, key string, at uint64) (datum.Row, bool) {
	v, ok := Visible(s.Tables[table][key], at)
	return datum.Clone(v.Row), ok
}
func (s *Store) Latest(table, key string) uint64 {
	v := s.Tables[table][key]
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1].Sequence
}
func (s *Store) Scan(table string, at uint64) map[string]datum.Row {
	out := map[string]datum.Row{}
	for k, c := range s.Tables[table] {
		if v, ok := Visible(c, at); ok {
			out[k] = datum.Clone(v.Row)
		}
	}
	return out
}
func (s *Store) Clone() *Store {
	out := New()
	for name, rows := range s.Tables {
		out.Ensure(name)
		for key, chain := range rows {
			for _, v := range chain {
				v.Row = datum.Clone(v.Row)
				out.Tables[name][key] = append(out.Tables[name][key], v)
			}
		}
	}
	return out
}
func (s *Store) Count() (keys, versions int) {
	for _, rows := range s.Tables {
		keys += len(rows)
		for _, v := range rows {
			versions += len(v)
		}
	}
	return
}

// EstimateMutation returns the accounting charge for one privately staged or
// durably committed mutation. Replacement of the same key is charged once per
// final write because only the last staged value is retained.
func EstimateMutation(m Mutation) int64 {
	size := mutationOverhead + int64(len(m.Relation)) + int64(len(m.Key))
	if m.Deleted {
		return size
	}
	return size + EstimateRow(m.Row)
}

// EstimateRow returns the accounting charge for one materialized row.
func EstimateRow(row datum.Row) int64 {
	size := rowOverhead
	for k, v := range row {
		size += columnOverhead + int64(len(k))
		switch v.Kind {
		case "text":
			size += int64(len(v.Text))
		case "int", "bool":
			size += 8
		}
	}
	return size
}

// EstimateVersions totals the charge for all versions currently retained in the
// store. It is used for diagnostics and restart reconciliation, never as a
// substitute for the live ledger maintained by the engine.
func (s *Store) EstimateVersions() int64 {
	var total int64
	for relation, rows := range s.Tables {
		for key, chain := range rows {
			for _, v := range chain {
				total += EstimateMutation(Mutation{Relation: relation, Key: key, Row: v.Row, Deleted: v.Deleted})
			}
		}
	}
	return total
}
