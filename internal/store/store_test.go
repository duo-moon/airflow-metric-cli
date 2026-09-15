package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
)

func TestUpsertAndSnapshot(t *testing.T) {
	t.Parallel()

	s := New()

	s.UpsertHealth(model.ClusterHealth{
		Components: []model.ComponentHealth{{Name: "Scheduler", Status: model.HealthHealthy}},
		ObservedAt: time.Now(),
	})
	if !s.Health().AllHealthy() {
		t.Errorf("expected AllHealthy() = true")
	}

	s.UpsertPools([]model.Pool{
		{Name: "b", Slots: 10, OccupiedSlots: 4},
		{Name: "a", Slots: 5},
	})
	pools := s.Pools()
	if len(pools) != 2 || pools[0].Name != "a" || pools[1].Name != "b" {
		t.Errorf("Pools() must be sorted by name, got %+v", pools)
	}

	s.UpsertImportErrors([]model.ImportError{
		{ID: 1, Filename: "old.py", Timestamp: time.Now().Add(-time.Hour)},
		{ID: 2, Filename: "new.py", Timestamp: time.Now()},
	})
	errs := s.ImportErrors()
	if len(errs) != 2 || errs[0].ID != 2 {
		t.Errorf("ImportErrors() must be newest-first, got %+v", errs)
	}
}

func TestDagRunsFilteringAndIncremental(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s := New()

	runs := []model.DagRun{
		{DagID: "d1", RunID: "r1", State: model.DagRunRunning, Start: now.Add(-2 * time.Minute), UpdatedAt: now.Add(-1 * time.Minute)},
		{DagID: "d1", RunID: "r2", State: model.DagRunQueued, Start: now.Add(-30 * time.Second), UpdatedAt: now.Add(-30 * time.Second)},
		{DagID: "d2", RunID: "r1", State: model.DagRunFailed, End: now.Add(-10 * time.Second), UpdatedAt: now.Add(-10 * time.Second)},
		{DagID: "d3", RunID: "r1", State: model.DagRunSuccess, End: now.Add(-1 * time.Hour), UpdatedAt: now.Add(-1 * time.Hour)},
	}
	s.UpsertDagRuns(runs)

	if got := s.DagRunCount(); got != 4 {
		t.Fatalf("DagRunCount = %d, want 4", got)
	}

	active := s.ActiveDagRuns()
	if len(active) != 2 {
		t.Errorf("ActiveDagRuns len = %d, want 2 (queued + running)", len(active))
	}
	if !active[0].Start.Before(active[1].Start) {
		t.Errorf("ActiveDagRuns must be sorted by Start asc")
	}

	failed := s.RecentFailedDagRunsSince(now, 1*time.Minute)
	if len(failed) != 1 || failed[0].DagID != "d2" {
		t.Errorf("RecentFailedDagRunsSince() = %+v, want [d2]", failed)
	}

	// updated_at_gte anchor is the max UpdatedAt (d2/r1 failed at -10s).
	if got := s.LatestDagRunUpdate(); !got.Equal(runs[2].UpdatedAt) {
		t.Errorf("LatestDagRunUpdate = %v, want %v", got, runs[2].UpdatedAt)
	}
}

func TestUpsertDagRuns_Overwrites(t *testing.T) {
	t.Parallel()

	s := New()
	s.UpsertDagRuns([]model.DagRun{
		{DagID: "d1", RunID: "r1", State: model.DagRunRunning, UpdatedAt: time.Now()},
	})
	s.UpsertDagRuns([]model.DagRun{
		{DagID: "d1", RunID: "r1", State: model.DagRunSuccess, UpdatedAt: time.Now()},
	})

	if s.DagRunCount() != 1 {
		t.Errorf("expected same key to overwrite, got count=%d", s.DagRunCount())
	}
	if s.ActiveDagRuns() != nil && len(s.ActiveDagRuns()) != 0 {
		t.Errorf("expected no active runs after success, got %+v", s.ActiveDagRuns())
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	// go test -race catches any missing lock.
	t.Parallel()

	s := New()
	now := time.Now()

	const iters = 200
	var wg sync.WaitGroup

	// Writers: DAG runs, pools, waiting counts, health, import errors.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				runID := fmt.Sprintf("r-%d-%d", shard, i)
				s.UpsertDagRuns([]model.DagRun{{
					DagID: "d", RunID: runID, State: model.DagRunRunning,
					Start: now, UpdatedAt: now.Add(time.Duration(i) * time.Millisecond),
				}})
				s.UpsertPools([]model.Pool{{Name: "p", Slots: i}})
				s.UpsertWaitingCounts([]model.WaitingCount{{DagID: "d", RunID: runID, Queued: 1}})
				s.UpsertHealth(model.ClusterHealth{ObservedAt: now})
				s.UpsertImportErrors([]model.ImportError{{ID: i, Filename: "x"}})
			}
		}(w)
	}

	// Readers: snapshots + point lookups.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = s.Health()
				_ = s.Pools()
				_ = s.ImportErrors()
				_ = s.ActiveDagRuns()
				_ = s.RecentFailedDagRunsSince(now, time.Hour)
				_ = s.WaitingCount("d", "r-0-0")
				_ = s.DagRunCount()
				_ = s.LatestDagRunUpdate()
			}
		}()
	}

	// One background pruner racing with the rest.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			s.PruneDagRunsBefore(now.Add(-time.Hour))
		}
	}()

	wg.Wait()

	// Sanity: store didn't wedge, LatestDagRunUpdate must have advanced.
	if s.LatestDagRunUpdate().IsZero() {
		t.Error("expected LatestDagRunUpdate to advance under concurrent load")
	}
}

func TestPruneDagRunsBefore(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s := New()
	s.UpsertDagRuns([]model.DagRun{
		{DagID: "d", RunID: "old-terminal", State: model.DagRunSuccess, UpdatedAt: now.Add(-2 * time.Hour)},
		{DagID: "d", RunID: "young-terminal", State: model.DagRunFailed, UpdatedAt: now.Add(-5 * time.Minute)},
		{DagID: "d", RunID: "old-running", State: model.DagRunRunning, UpdatedAt: now.Add(-2 * time.Hour)},
	})

	dropped := s.PruneDagRunsBefore(now.Add(-1 * time.Hour))
	if dropped != 1 {
		t.Errorf("dropped = %d, want 1 (only old-terminal)", dropped)
	}
	if s.DagRunCount() != 2 {
		t.Errorf("remaining count = %d, want 2 (young-terminal + old-running kept)", s.DagRunCount())
	}
}
