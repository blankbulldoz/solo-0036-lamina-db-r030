package index

import (
	"lamina/internal/datum"
	"sort"
)

// entryOverhead accounts for the slice entry, sortable value and duplicate
// detection slot retained per candidate. It is a deterministic accounting
// unit, not a measurement of Go allocator overhead.
const entryOverhead int64 = 96

type Entry struct {
	Value datum.Value
	Key   string
}
type Tree struct {
	entries []Entry
	seen    map[string]bool
	bytes   int64
}

func New() *Tree { return &Tree{seen: map[string]bool{}} }

func token(value datum.Value, key string) string { return value.Key() + "\x00" + key }

// EstimateEntry returns the charge for retaining one candidate association.
func EstimateEntry(value datum.Value, key string) int64 {
	size := entryOverhead + int64(len(key))
	if value.Kind == "text" {
		size += int64(len(value.Text))
	}
	return size
}

// WouldAdd reports whether Add would retain new state. Historical candidates
// deduplicate on value+primary, so replacement commits often add nothing.
func (t *Tree) WouldAdd(value datum.Value, key string) bool {
	if value.Null() {
		return false
	}
	return !t.seen[token(value, key)]
}
func (t *Tree) Add(value datum.Value, key string) {
	if value.Null() {
		return
	}
	raw := token(value, key)
	if t.seen[raw] {
		return
	}
	t.seen[raw] = true
	t.bytes += EstimateEntry(value, key)
	at := sort.Search(len(t.entries), func(i int) bool {
		n := datum.Order(t.entries[i].Value, value)
		return n > 0 || (n == 0 && t.entries[i].Key >= key)
	})
	t.entries = append(t.entries, Entry{})
	copy(t.entries[at+1:], t.entries[at:])
	t.entries[at] = Entry{value, key}
}
func (t *Tree) Candidates(op string, value datum.Value) []string {
	if value.Null() {
		return nil
	}
	lo := sort.Search(len(t.entries), func(i int) bool { return datum.Order(t.entries[i].Value, value) >= 0 })
	hi := sort.Search(len(t.entries), func(i int) bool { return datum.Order(t.entries[i].Value, value) > 0 })
	begin, end := 0, len(t.entries)
	switch op {
	case "=":
		begin, end = lo, hi
	case ">":
		begin = hi
	case ">=":
		begin = lo
	case "<":
		end = lo
	case "<=":
		end = hi
	default:
		return nil
	}
	out := make([]string, 0, end-begin)
	seen := map[string]bool{}
	for _, e := range t.entries[begin:end] {
		if !seen[e.Key] {
			seen[e.Key] = true
			out = append(out, e.Key)
		}
	}
	return out
}
func (t *Tree) Size() int    { return len(t.entries) }
func (t *Tree) Bytes() int64 { return t.bytes }
