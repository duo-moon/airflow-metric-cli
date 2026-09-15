package poller

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoller_AddAfterRunPanics(t *testing.T) {
	t.Parallel()

	p := New()
	p.Add(Task{
		Name:     "seed",
		Interval: 5 * time.Millisecond,
		Fn:       func(_ context.Context) error { return nil },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Give Run() a moment to flip the running flag.
	time.Sleep(5 * time.Millisecond)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on Add after Run")
		}
		cancel()
		<-done
	}()

	p.Add(Task{Name: "late", Interval: time.Second, Fn: func(_ context.Context) error { return nil }})
}

func TestNextInterval_FloorClamp(t *testing.T) {
	t.Parallel()

	// jitter equal to interval: without floor-clamp, delta could land in
	// [-interval/2, +interval/2), producing 0. Verify we stay >= interval/2.
	interval := 100 * time.Millisecond
	for i := 0; i < 200; i++ {
		got := nextInterval(interval, interval)
		if got < interval/2 {
			t.Fatalf("nextInterval below floor: got %v (want >= %v)", got, interval/2)
		}
	}
}

func TestGuard_EndToEndInsideRunningPoller(t *testing.T) {
	t.Parallel()

	// Task that always errors — Guard should notice within the first tick
	// window and double the interval.
	base := 5 * time.Millisecond
	var runs atomic.Int32
	p := New()
	p.Add(Task{
		Name:     "flaky",
		Interval: base,
		Fn: func(_ context.Context) error {
			runs.Add(1)
			return errors.New("boom")
		},
	})

	g := NewGuard(p, 100*time.Millisecond, 0.5)
	g.Watch("flaky", base)
	p.Add(g.Task(20 * time.Millisecond)) // guard tick inside the run window

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Guard should have raised the interval above base at least once.
	if got := p.CurrentInterval("flaky"); got <= base {
		t.Errorf("expected interval to grow above base=%v under sustained errors, got %v", base, got)
	}
	if runs.Load() == 0 {
		t.Error("flaky task never ran")
	}
}

func TestMetrics_LastErrOverwrites(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.record("t", time.Millisecond, errors.New("first"))
	m.record("t", time.Millisecond, errors.New("second"))
	m.record("t", time.Millisecond, nil) // success does NOT clear LastError

	snap := m.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("len = %d", len(snap))
	}
	if snap[0].LastError != "second" {
		t.Errorf("LastError = %q, want %q", snap[0].LastError, "second")
	}
	if snap[0].Errors != 2 || snap[0].Runs != 3 {
		t.Errorf("counts wrong: runs=%d errors=%d", snap[0].Runs, snap[0].Errors)
	}
}

func TestMetrics_SnapshotSortedByName(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.record("beta", time.Millisecond, nil)
	m.record("alpha", time.Millisecond, nil)
	m.record("gamma", time.Millisecond, nil)

	snap := m.Snapshot()
	got := []string{snap[0].Name, snap[1].Name, snap[2].Name}
	want := []string{"alpha", "beta", "gamma"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Snapshot[%d].Name = %q, want %q (got=%v)", i, got[i], want[i], got)
		}
	}
}
