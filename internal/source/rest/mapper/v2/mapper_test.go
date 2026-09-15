package v2

import (
	"testing"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/airflowv2"
)

func ptr[T any](v T) *T { return &v }

func TestClusterHealth_AllComponents(t *testing.T) {
	t.Parallel()

	healthy := "healthy"
	unhealthy := "unhealthy"
	weird := "MYSTERY"
	hb := ptr("2026-08-17T18:00:00Z")

	in := &airflowv2.HealthInfoResponse{
		Scheduler:    airflowv2.SchedulerInfoResponse{Status: &healthy, LatestSchedulerHeartbeat: hb},
		Metadatabase: airflowv2.BaseInfoResponse{Status: &unhealthy},
		Triggerer:    airflowv2.TriggererInfoResponse{Status: &weird},
	}
	obs := time.Now()
	got := ClusterHealth(in, obs)

	if len(got.Components) != 3 {
		t.Fatalf("components len = %d, want 3 (dag processor absent)", len(got.Components))
	}
	byName := make(map[string]model.ComponentHealth, len(got.Components))
	for _, c := range got.Components {
		byName[c.Name] = c
	}
	if byName["Scheduler"].Status != model.HealthHealthy {
		t.Errorf("Scheduler status = %s", byName["Scheduler"].Status)
	}
	if byName["Database"].Status != model.HealthUnhealthy {
		t.Errorf("Database status = %s", byName["Database"].Status)
	}
	if byName["Triggerer"].Status != model.HealthUnknown {
		t.Errorf("case-insensitive mystery status must fold to Unknown, got %s", byName["Triggerer"].Status)
	}
	if got.ObservedAt != obs {
		t.Error("ObservedAt not preserved")
	}
}

func TestClusterHealth_Nil(t *testing.T) {
	t.Parallel()
	got := ClusterHealth(nil, time.Now())
	if len(got.Components) != 0 {
		t.Errorf("nil HealthInfoResponse must yield no components, got %+v", got.Components)
	}
}

func TestDagRun_FieldsAndUpdatedAt(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	end := start.Add(5 * time.Minute)
	state := airflowv2.DagRunState("running")
	got := DagRun(&airflowv2.DAGRunResponse{
		DagId:     "d",
		DagRunId:  "r-1",
		State:     state,
		StartDate: &start,
		EndDate:   &end,
	})
	if got.DagID != "d" || got.RunID != "r-1" {
		t.Errorf("keys: got %s/%s", got.DagID, got.RunID)
	}
	if got.State != model.DagRunRunning {
		t.Errorf("State = %s", got.State)
	}
	if !got.UpdatedAt.Equal(end) {
		t.Errorf("UpdatedAt should fall back to end date, got %v want %v", got.UpdatedAt, end)
	}
}

func TestDagRun_NilInputZero(t *testing.T) {
	t.Parallel()

	// v2 has no nil-return convention; a nil input yields a zero DagRun so
	// callers can safely append without a guard. Batch mapper keeps every
	// row (no skip semantics).
	if got := DagRun(nil); got != (model.DagRun{}) {
		t.Errorf("DagRun(nil) = %+v, want zero-value", got)
	}
	batch := DagRuns([]airflowv2.DAGRunResponse{
		{DagId: "d1", DagRunId: "r1"},
		{DagId: "d2", DagRunId: "r2"},
	})
	if len(batch) != 2 {
		t.Errorf("DagRuns kept %d rows, want 2 (no skip semantics)", len(batch))
	}
}

func TestPool_AllFields(t *testing.T) {
	t.Parallel()
	got := Pool(&airflowv2.PoolResponse{
		Name:          "default",
		Slots:         10,
		OccupiedSlots: 4,
		DeferredSlots: 2,
		Description:   ptr("desc"),
	})
	if got.Name != "default" || got.Slots != 10 || got.DeferredSlots != 2 {
		t.Errorf("unexpected pool: %+v", got)
	}
	if got.Description != "desc" {
		t.Errorf("description = %q", got.Description)
	}
}

func TestImportError_Timestamp(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	got := ImportError(&airflowv2.ImportErrorResponse{
		ImportErrorId: 42,
		Filename:      "dags/bad.py",
		StackTrace:    "Traceback…",
		Timestamp:     ts,
	})
	if got.ID != 42 || got.Filename != "dags/bad.py" {
		t.Errorf("unexpected: %+v", got)
	}
	if !got.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", got.Timestamp, ts)
	}
}

func TestTaskInstance_DurationFallback(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	end := start.Add(90 * time.Second)
	got := TaskInstance(&airflowv2.TaskInstanceResponse{
		DagId:     "d",
		DagRunId:  "r",
		TaskId:    "t",
		StartDate: &start,
		EndDate:   &end,
		// Duration deliberately unset — expect fallback via End-Start.
	})
	if got.Duration != 90*time.Second {
		t.Errorf("Duration fallback = %v, want 90s", got.Duration)
	}
}

func TestTaskInstance_TypedState(t *testing.T) {
	t.Parallel()
	state := airflowv2.TaskInstanceState("upstream_failed")
	got := TaskInstance(&airflowv2.TaskInstanceResponse{
		DagId: "d", DagRunId: "r", TaskId: "t", State: &state,
	})
	if got.State != model.TaskUpstreamFailed {
		t.Errorf("State = %q, want TaskUpstreamFailed", got.State)
	}
}

func TestTaskAttempts_SortedAscending(t *testing.T) {
	t.Parallel()

	state := airflowv2.TaskInstanceState("failed")
	in := []airflowv2.TaskInstanceHistoryResponse{
		{TryNumber: 3, State: &state},
		{TryNumber: 1, State: &state},
		{TryNumber: 2, State: &state},
	}
	got := TaskAttempts(in)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	for i, want := range []int{1, 2, 3} {
		if got[i].TryNumber != want {
			t.Errorf("got[%d].TryNumber = %d, want %d", i, got[i].TryNumber, want)
		}
	}
}

func TestAggregateWaiting_GroupsAndCounts(t *testing.T) {
	t.Parallel()

	states := []airflowv2.TaskInstanceState{"queued", "queued", "up_for_retry", "deferred", "running"}
	items := make([]airflowv2.TaskInstanceResponse, len(states))
	for i := range states {
		items[i] = airflowv2.TaskInstanceResponse{
			DagId: "d", DagRunId: "r", TaskId: "t", State: &states[i],
		}
	}
	got := AggregateWaiting(items)
	if len(got) != 1 {
		t.Fatalf("expected 1 aggregated key, got %d", len(got))
	}
	w := got[0]
	if w.DagID != "d" || w.RunID != "r" {
		t.Errorf("keys wrong: %+v", w)
	}
	// running is not a waiting state — must be ignored.
	if w.Queued != 2 || w.Retry != 1 || w.Deferred != 1 || w.Total() != 4 {
		t.Errorf("counts wrong: %+v (total=%d)", w, w.Total())
	}
}
