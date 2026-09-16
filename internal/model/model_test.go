package model

import (
	"testing"
	"time"
)

func TestDagRunState_IsTerminal(t *testing.T) {
	t.Parallel()
	cases := map[DagRunState]bool{
		DagRunSuccess:               true,
		DagRunFailed:                true,
		DagRunRunning:               false,
		DagRunQueued:                false,
		DagRunState("future_state"): false,
	}
	for s, want := range cases {
		if got := s.IsTerminal(); got != want {
			t.Errorf("%q.IsTerminal() = %v, want %v", s, got, want)
		}
	}
}

func TestDagRun_Duration(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Both start and end set — plain subtraction.
	completed := DagRun{Start: base, End: base.Add(5 * time.Minute)}
	if got := completed.Duration(); got != 5*time.Minute {
		t.Errorf("completed.Duration() = %v, want 5m", got)
	}

	// End zero — falls back to elapsed since Start.
	running := DagRun{Start: time.Now().Add(-3 * time.Second)}
	if got := running.Duration(); got < 2*time.Second || got > 10*time.Second {
		t.Errorf("running.Duration() = %v, want ≈3s", got)
	}

	// Zero Start — no meaningful duration.
	if got := (DagRun{}).Duration(); got != 0 {
		t.Errorf("zero-Start duration must be 0, got %v", got)
	}
}

func TestWaitingCount_Total(t *testing.T) {
	t.Parallel()

	w := WaitingCount{
		Reschedule: 1,
		Retry:      2,
		Scheduled:  3,
		Queued:     4,
		Deferred:   5,
	}
	if got := w.Total(); got != 15 {
		t.Errorf("Total() = %d, want 15", got)
	}
	if (WaitingCount{}).Total() != 0 {
		t.Error("zero-value Total() must be 0")
	}
}

func TestTaskState_IsTerminal(t *testing.T) {
	t.Parallel()
	cases := map[TaskState]bool{
		TaskSuccess:                       true,
		TaskFailed:                        true,
		TaskUpstreamFailed:                true,
		TaskSkipped:                       true,
		TaskRemoved:                       true,
		TaskRunning:                       false,
		TaskUpForRetry:                    false, // retry means another attempt is coming
		TaskUpForReschedule:               false,
		TaskDeferred:                      false,
		TaskQueued:                        false,
		TaskScheduled:                     false,
		TaskRestarting:                    false,
		TaskState("unknown_future_state"): false,
	}
	for s, want := range cases {
		if got := s.IsTerminal(); got != want {
			t.Errorf("%q.IsTerminal() = %v, want %v", s, got, want)
		}
	}
}

func TestClusterHealth_UnhealthyExcludesUnknown(t *testing.T) {
	t.Parallel()
	h := ClusterHealth{Components: []ComponentHealth{
		{Name: "Scheduler", Status: HealthHealthy},
		{Name: "Database", Status: HealthUnknown}, // no data — not a page-worthy alert
		{Name: "Triggerer", Status: HealthUnhealthy},
	}}

	bad := h.Unhealthy()
	if len(bad) != 1 || bad[0].Name != "Triggerer" {
		t.Errorf("Unhealthy() = %+v, want just Triggerer", bad)
	}
	if h.AllHealthy() {
		t.Error("AllHealthy() must be false when any component is unhealthy")
	}
}

func TestClusterHealth_AllHealthyEmpty(t *testing.T) {
	t.Parallel()
	if (ClusterHealth{}).AllHealthy() {
		t.Error("empty ClusterHealth must not report AllHealthy")
	}
}

func TestPool_UtilizationIncludesDeferred(t *testing.T) {
	t.Parallel()
	p := Pool{Slots: 10, OccupiedSlots: 4, DeferredSlots: 3}
	got := p.Utilization()
	if got < 0.69 || got > 0.71 {
		t.Errorf("Utilization = %f, want ≈0.7 (4+3 of 10)", got)
	}
	if p.UsedSlots() != 7 {
		t.Errorf("UsedSlots = %d, want 7", p.UsedSlots())
	}
}

func TestPool_UtilizationClampsAndZero(t *testing.T) {
	t.Parallel()
	if u := (Pool{Slots: 0}).Utilization(); u != 0 {
		t.Errorf("zero-slot pool must report 0 utilization, got %f", u)
	}
	// If Airflow reports occupied > slots (rare, but not impossible during
	// scale-down), we clamp to 1.
	over := Pool{Slots: 4, OccupiedSlots: 6, DeferredSlots: 2}
	if u := over.Utilization(); u != 1 {
		t.Errorf("over-provisioned pool must clamp to 1, got %f", u)
	}
}
