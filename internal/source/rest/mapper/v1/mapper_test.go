package v1

import (
	"testing"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/airflowv1"
)

func ptr[T any](v T) *T { return &v }

func TestClusterHealth_AllComponents(t *testing.T) {
	t.Parallel()

	heartbeat := "2026-08-17T18:00:00Z"
	healthy := airflowv1.HealthStatus("healthy")
	unhealthy := airflowv1.HealthStatus("unhealthy")
	weird := airflowv1.HealthStatus("mystery")

	in := &airflowv1.HealthInfo{
		Scheduler:    &airflowv1.SchedulerStatus{Status: &healthy, LatestSchedulerHeartbeat: &heartbeat},
		Metadatabase: &airflowv1.MetadatabaseStatus{Status: &unhealthy},
		Triggerer:    &airflowv1.TriggererStatus{Status: &weird},
	}
	obs := time.Now()
	got := ClusterHealth(in, obs)

	if len(got.Components) != 3 {
		t.Fatalf("components len = %d, want 3", len(got.Components))
	}
	byName := make(map[string]model.ComponentHealth, len(got.Components))
	for _, c := range got.Components {
		byName[c.Name] = c
	}
	if byName["Scheduler"].Status != model.HealthHealthy {
		t.Errorf("Scheduler = %s", byName["Scheduler"].Status)
	}
	if byName["Database"].Status != model.HealthUnhealthy {
		t.Errorf("Database = %s", byName["Database"].Status)
	}
	if byName["Triggerer"].Status != model.HealthUnknown {
		t.Errorf("unrecognized status must map to Unknown, got %s", byName["Triggerer"].Status)
	}
	if got.ObservedAt != obs {
		t.Errorf("ObservedAt not preserved")
	}
	wantHB, _ := time.Parse(time.RFC3339, heartbeat)
	if !byName["Scheduler"].LatestHeartbeat.Equal(wantHB) {
		t.Errorf("heartbeat = %v, want %v", byName["Scheduler"].LatestHeartbeat, wantHB)
	}
}

func TestClusterHealth_Nil(t *testing.T) {
	t.Parallel()
	got := ClusterHealth(nil, time.Now())
	if len(got.Components) != 0 {
		t.Errorf("nil HealthInfo must produce empty Components, got %+v", got.Components)
	}
}

func TestDagRun_RequiredKeys(t *testing.T) {
	t.Parallel()

	if DagRun(nil) != nil {
		t.Error("nil DAGRun must map to nil")
	}
	if DagRun(&airflowv1.DAGRun{DagId: ptr("d")}) != nil {
		t.Error("missing DagRunId must map to nil")
	}
	if DagRun(&airflowv1.DAGRun{DagRunId: ptr("r")}) != nil {
		t.Error("missing DagId must map to nil")
	}
}

func TestDagRun_FieldsAndUpdatedAt(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	end := start.Add(5 * time.Minute)
	state := airflowv1.DagState("running")
	rt := airflowv1.DAGRunRunType("manual")

	got := DagRun(&airflowv1.DAGRun{
		DagId:     ptr("d"),
		DagRunId:  ptr("r1"),
		State:     &state,
		StartDate: &start,
		EndDate:   &end,
		RunType:   &rt,
	})
	if got == nil {
		t.Fatal("expected non-nil")
	}
	if got.State != model.DagRunRunning {
		t.Errorf("State = %s", got.State)
	}
	if got.RunType != "manual" {
		t.Errorf("RunType = %s", got.RunType)
	}
	if !got.UpdatedAt.Equal(end) {
		t.Errorf("UpdatedAt should fall back to EndDate: got %v want %v", got.UpdatedAt, end)
	}
}

func TestDagRuns_SkipsInvalid(t *testing.T) {
	t.Parallel()

	got := DagRuns([]airflowv1.DAGRun{
		{DagId: ptr("d1"), DagRunId: ptr("r1")},
		{DagId: ptr("d2")}, // no run id — dropped
		{DagId: ptr("d3"), DagRunId: ptr("r3")},
	})
	if len(got) != 2 {
		t.Errorf("len = %d, want 2 (one dropped)", len(got))
	}
}

func TestPool(t *testing.T) {
	t.Parallel()

	if Pool(nil) != nil || Pool(&airflowv1.Pool{}) != nil {
		t.Error("missing name must map to nil")
	}
	got := Pool(&airflowv1.Pool{
		Name:          ptr("default"),
		Slots:         ptr(10),
		OccupiedSlots: ptr(3),
		RunningSlots:  ptr(2),
		QueuedSlots:   ptr(1),
		OpenSlots:     ptr(7),
	})
	if got.Name != "default" || got.Slots != 10 || got.OpenSlots != 7 {
		t.Errorf("unexpected pool: %+v", got)
	}
	if u := got.Utilization(); u < 0.29 || u > 0.31 {
		t.Errorf("Utilization = %f, want ~0.3", u)
	}
}

func TestImportError(t *testing.T) {
	t.Parallel()

	ts := "2026-08-17T18:00:00Z"
	got := ImportError(&airflowv1.ImportError{
		ImportErrorId: ptr(42),
		Filename:      ptr("dags/bad.py"),
		StackTrace:    ptr("Traceback..."),
		Timestamp:     &ts,
	})
	if got == nil || got.ID != 42 || got.Filename != "dags/bad.py" {
		t.Errorf("unexpected: %+v", got)
	}
	if got.Timestamp.IsZero() {
		t.Error("Timestamp must be parsed")
	}
}

func TestTaskInstance_DurationFallback(t *testing.T) {
	t.Parallel()

	start := "2026-08-17T10:00:00Z"
	end := "2026-08-17T10:01:30Z"
	got := TaskInstance(&airflowv1.TaskInstance{
		DagId:     ptr("d"),
		DagRunId:  ptr("r"),
		TaskId:    ptr("t"),
		StartDate: &start,
		EndDate:   &end,
		// Duration deliberately unset — expect fallback via End-Start.
	})
	if got == nil {
		t.Fatal("expected non-nil TaskInstance")
	}
	if got.Duration != 90*time.Second {
		t.Errorf("Duration fallback = %v, want 90s", got.Duration)
	}
}

func TestTaskInstance_TypedState(t *testing.T) {
	t.Parallel()

	state := airflowv1.TaskState("upstream_failed")
	got := TaskInstance(&airflowv1.TaskInstance{
		DagId: ptr("d"), DagRunId: ptr("r"), TaskId: ptr("t"), State: &state,
	})
	if got == nil {
		t.Fatal("expected non-nil TaskInstance")
	}
	if got.State != model.TaskUpstreamFailed {
		t.Errorf("State = %q, want TaskUpstreamFailed", got.State)
	}
}

func TestTaskAttempts_SortedAscending(t *testing.T) {
	t.Parallel()

	state := airflowv1.TaskState("failed")
	in := []airflowv1.TaskInstanceHistory{
		{TryNumber: ptr(3), State: &state},
		{TryNumber: ptr(1), State: &state},
		{TryNumber: ptr(2), State: &state},
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

	states := []airflowv1.TaskState{"queued", "queued", "up_for_retry", "deferred", "running"}
	items := make([]airflowv1.TaskInstance, len(states))
	for i := range states {
		items[i] = airflowv1.TaskInstance{
			DagId: ptr("d"), DagRunId: ptr("r"), TaskId: ptr("t"), State: &states[i],
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
