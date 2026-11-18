package versions

type VacuumResult struct {
	Horizon  uint64 `json:"horizon"`
	Removed  int    `json:"removed"`
	Retained int    `json:"retained"`
	Pinned   bool   `json:"pinned"`
	// ReclaimedBytes is the ledger charge returned by this physical removal.
	// Removed counts versions regardless of whether they were charged to this
	// instance; ReclaimedBytes is what the instance can actually spend again.
	ReclaimedBytes int64 `json:"reclaimed_bytes"`
	// IndexReclaimedBytes is the candidate-state reduction from rebuilding
	// indexes over the retained versions. It is capacity returned as well, but
	// it is reported separately from version rows.
	IndexReclaimedBytes int64 `json:"index_reclaimed_bytes"`
	// ReclaimableBytes is the additional version charge that a vacuum at the
	// currently safe horizon could return. It is reported by capacity refusals
	// so a caller can tell "history exists but a long reader pins it" from
	// "there is no reclaimable history at all".
	ReclaimableBytes int64 `json:"reclaimable_bytes,omitempty"`
	// InstanceBytes is the ledger total after this request, so a caller can
	// see that a successful vacuum request which removed nothing did not
	// return any capacity.
	InstanceBytes int64 `json:"instance_bytes,omitempty"`
	// IndexBytes is the rebuilt candidate state after reclamation. Rebuilding
	// can shrink index candidates too; that reduction is included in the
	// instance total but deliberately not in ReclaimedBytes, which counts only
	// version rows.
	IndexBytes int64          `json:"index_bytes,omitempty"`
	PinnedBy   map[string]any `json:"pinned_by,omitempty"`
}

// PruneCandidateBytes measures, without mutating, how many ledger bytes a
// Vacuum at horizon would return. Admission refusals use it to tell the caller
// whether reclamation could help and how much history is actually removable.
func (s *Store) PruneCandidateBytes(horizon uint64) int64 {
	var total int64
	for relation, rows := range s.Tables {
		for key, chain := range rows {
			keep := 0
			for keep+1 < len(chain) && chain[keep+1].Sequence <= horizon {
				keep++
			}
			drop := keep
			if len(chain) > 0 && chain[len(chain)-1].Sequence <= horizon && chain[len(chain)-1].Deleted {
				drop = len(chain)
			}
			for _, v := range chain[:drop] {
				total += EstimateMutation(Mutation{Relation: relation, Key: key, Row: v.Row, Deleted: v.Deleted})
			}
		}
	}
	return total
}

// Vacuum removes historical versions at or below horizon using the existing
// retention rule: keep the last version at the horizon plus every later one,
// and drop a fully expired tombstone chain. It returns both the count and the
// byte charge of the versions it actually deleted.
func (s *Store) Vacuum(horizon uint64) VacuumResult {
	result := VacuumResult{Horizon: horizon}
	for relation, rows := range s.Tables {
		for key, chain := range rows {
			keep := 0
			for keep+1 < len(chain) && chain[keep+1].Sequence <= horizon {
				keep++
			}
			if len(chain) > 0 && chain[len(chain)-1].Sequence <= horizon && chain[len(chain)-1].Deleted {
				for _, v := range chain {
					result.ReclaimedBytes += EstimateMutation(Mutation{Relation: relation, Key: key, Row: v.Row, Deleted: v.Deleted})
				}
				result.Removed += len(chain)
				delete(rows, key)
				continue
			}
			for _, v := range chain[:keep] {
				result.ReclaimedBytes += EstimateMutation(Mutation{Relation: relation, Key: key, Row: v.Row, Deleted: v.Deleted})
			}
			result.Removed += keep
			remaining := append([]Version(nil), chain[keep:]...)
			rows[key] = remaining
			result.Retained += len(remaining)
		}
	}
	return result
}
