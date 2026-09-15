// Package store is a thread-safe in-memory cache of Airflow state observed
// by collectors. Snapshot methods return sorted copies so UI code can walk
// them without a lock.
//
// Ownership semantics:
//
//   - health, pools, importErrors, waiting — full replacement on every
//     Upsert. Ghost entries from previous polls clean themselves; no
//     explicit prune is needed.
//   - dagRuns — merged by (dag_id, run_id). A batched poll returning
//     top-N by start_date can silently drop older runs; the store keeps
//     them so long as PruneDagRunsBefore hasn't reached them.
//
// The store never calls time.Now(); accessors that need a "now" reference
// take it as an argument, which keeps everything trivially testable.
package store

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
)

// Store owns the current view of the cluster.
type Store struct {
	mu           sync.RWMutex
	health       model.ClusterHealth
	pools        map[string]model.Pool
	importErrors map[int]model.ImportError
	dagRuns      map[dagRunKey]model.DagRun
	waiting      map[dagRunKey]model.WaitingCount
	latestUpdate time.Time // max DagRun.UpdatedAt observed — feeds incremental polling
}

type dagRunKey struct {
	dagID string
	runID string
}

// New returns an empty Store.
func New() *Store {
	return &Store{
		pools:        make(map[string]model.Pool),
		importErrors: make(map[int]model.ImportError),
		dagRuns:      make(map[dagRunKey]model.DagRun),
		waiting:      make(map[dagRunKey]model.WaitingCount),
	}
}

// UpsertHealth replaces the last known health snapshot.
func (s *Store) UpsertHealth(h model.ClusterHealth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.health = h
}

// Health returns the last known cluster health.
func (s *Store) Health() model.ClusterHealth {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.health
}

// UpsertPools replaces the current pool set with the given slice.
func (s *Store) UpsertPools(ps []model.Pool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pools = make(map[string]model.Pool, len(ps))
	for _, p := range ps {
		s.pools[p.Name] = p
	}
}

// Pools returns pools sorted by name.
func (s *Store) Pools() []model.Pool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Pool, 0, len(s.pools))
	for _, p := range s.pools {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b model.Pool) int {
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

// UpsertImportErrors replaces the current set of DAG parse errors.
func (s *Store) UpsertImportErrors(es []model.ImportError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.importErrors = make(map[int]model.ImportError, len(es))
	for _, e := range es {
		s.importErrors[e.ID] = e
	}
}

// ImportErrors returns errors sorted by Timestamp descending (newest first).
func (s *Store) ImportErrors() []model.ImportError {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.ImportError, 0, len(s.importErrors))
	for _, e := range s.importErrors {
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b model.ImportError) int {
		return b.Timestamp.Compare(a.Timestamp)
	})
	return out
}

// UpsertDagRuns merges the given runs by (DagID, RunID). Existing runs with
// the same key are overwritten. Tracks the max UpdatedAt for incremental
// polling via LatestDagRunUpdate().
func (s *Store) UpsertDagRuns(runs []model.DagRun) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range runs {
		s.dagRuns[dagRunKey{r.DagID, r.RunID}] = r
		if r.UpdatedAt.After(s.latestUpdate) {
			s.latestUpdate = r.UpdatedAt
		}
	}
}

// LatestDagRunUpdate returns the max UpdatedAt among all stored DagRuns, or
// the zero value if none. Collectors pass this back to Airflow as
// updated_at_gte for incremental polling.
func (s *Store) LatestDagRunUpdate() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latestUpdate
}

// ActiveDagRuns returns runs currently in queued or running state, sorted by
// Start ascending (oldest first).
func (s *Store) ActiveDagRuns() []model.DagRun {
	return s.filterDagRuns(func(r model.DagRun) bool {
		return r.State == model.DagRunQueued || r.State == model.DagRunRunning
	}, func(a, b model.DagRun) int {
		return a.Start.Compare(b.Start)
	})
}

// RecentFailedDagRunsSince returns failed runs whose End is > now-window,
// sorted by End descending (most recent first). Callers pass `now` so the
// function stays pure and testable.
func (s *Store) RecentFailedDagRunsSince(now time.Time, window time.Duration) []model.DagRun {
	cutoff := now.Add(-window)
	return s.filterDagRuns(func(r model.DagRun) bool {
		return r.State == model.DagRunFailed && r.End.After(cutoff)
	}, func(a, b model.DagRun) int {
		return b.End.Compare(a.End)
	})
}

// UpsertWaitingCounts replaces the entire waiting-count map. Callers supply
// the full picture from one batched request so ghost entries for runs that
// no longer have waiting tasks are dropped automatically.
func (s *Store) UpsertWaitingCounts(counts []model.WaitingCount) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waiting = make(map[dagRunKey]model.WaitingCount, len(counts))
	for _, w := range counts {
		s.waiting[dagRunKey{w.DagID, w.RunID}] = w
	}
}

// WaitingCount returns the aggregated waiting counts for one DAG run.
// Zero-value if none recorded.
func (s *Store) WaitingCount(dagID, runID string) model.WaitingCount {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.waiting[dagRunKey{dagID, runID}]
}

// PruneDagRunsBefore drops terminal DagRuns whose UpdatedAt is before the
// given time. Non-terminal runs are always kept, regardless of age. Returns
// the number of runs dropped.
//
// Only dagRuns needs pruning — the other collections use full-replace
// upsert and clean themselves.
func (s *Store) PruneDagRunsBefore(t time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	before := len(s.dagRuns)
	for k, r := range s.dagRuns {
		if r.State.IsTerminal() && r.UpdatedAt.Before(t) {
			delete(s.dagRuns, k)
		}
	}
	return before - len(s.dagRuns)
}

// DagRunCount returns the number of DagRuns currently held. Exposed for
// tests and the Pruner cadence; other collections don't need a dedicated
// counter — use len(Snapshot()) instead.
func (s *Store) DagRunCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.dagRuns)
}

func (s *Store) filterDagRuns(pred func(model.DagRun) bool, less func(a, b model.DagRun) int) []model.DagRun {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.DagRun, 0, len(s.dagRuns))
	for _, r := range s.dagRuns {
		if pred(r) {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, less)
	return out
}
