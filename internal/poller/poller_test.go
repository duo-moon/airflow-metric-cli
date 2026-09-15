package poller

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoller_RunFiresRepeatedly(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	p := New()
	p.Add(Task{
		Name:     "tick",
		Interval: 5 * time.Millisecond,
		Fn: func(_ context.Context) error {
			calls.Add(1)
			return nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := calls.Load(); got < 3 {
		t.Fatalf("expected at least 3 fires in 60ms with 5ms interval, got %d", got)
	}

	snap := p.Metrics().Snapshot()
	if len(snap) != 1 {
		t.Fatalf("snapshot len = %d, want 1", len(snap))
	}
	if snap[0].Runs != uint64(calls.Load()) {
		t.Errorf("metrics Runs=%d, actual calls=%d", snap[0].Runs, calls.Load())
	}
	if snap[0].Errors != 0 {
		t.Errorf("Errors = %d, want 0", snap[0].Errors)
	}
}

func TestPoller_ContextCancelStopsQuickly(t *testing.T) {
	t.Parallel()

	p := New()
	p.Add(Task{
		Name:     "slow",
		Interval: 10 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
				return nil
			}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	time.Sleep(15 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on graceful shutdown", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not stop within 200ms after cancel")
	}

	// Cancellation propagated through Fn must NOT increment Errors.
	snap := p.Metrics().Snapshot()
	if len(snap) == 0 {
		return // task may not have recorded anything if cancel raced first tick
	}
	if snap[0].Errors != 0 {
		t.Errorf("Errors = %d, want 0 (ctx.Canceled is not a task error)", snap[0].Errors)
	}
}

func TestPoller_RecordsErrors(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	p := New()
	p.Add(Task{
		Name:     "err",
		Interval: 5 * time.Millisecond,
		Fn:       func(_ context.Context) error { return sentinel },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	snap := p.Metrics().Snapshot()
	if len(snap) != 1 {
		t.Fatalf("snapshot len = %d, want 1", len(snap))
	}
	if snap[0].Errors == 0 || snap[0].Errors != snap[0].Runs {
		t.Errorf("expected every run to error: runs=%d errors=%d", snap[0].Runs, snap[0].Errors)
	}
	if snap[0].LastError != "boom" {
		t.Errorf("LastError = %q, want %q", snap[0].LastError, "boom")
	}
}

func TestPoller_EmptyReturnsError(t *testing.T) {
	t.Parallel()

	err := New().Run(context.Background())
	if err == nil {
		t.Fatal("expected error for empty poller")
	}
}

func TestPoller_MultipleTasksIndependent(t *testing.T) {
	t.Parallel()

	var fastCalls, slowCalls atomic.Int32
	p := New()
	p.Add(Task{Name: "fast", Interval: 5 * time.Millisecond, Fn: func(_ context.Context) error {
		fastCalls.Add(1)
		return nil
	}})
	p.Add(Task{Name: "slow", Interval: 25 * time.Millisecond, Fn: func(_ context.Context) error {
		slowCalls.Add(1)
		return nil
	}})

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if fastCalls.Load() <= slowCalls.Load() {
		t.Errorf("fast=%d should exceed slow=%d given 5ms vs 25ms interval",
			fastCalls.Load(), slowCalls.Load())
	}
}

func TestMetrics_Percentiles(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	for i := 1; i <= 100; i++ {
		m.record("q", time.Duration(i)*time.Millisecond, nil)
	}

	snap := m.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("snapshot len = %d", len(snap))
	}
	// Ring stores last min(100, 128) samples, sorted values are 1..100 ms.
	// p50 = element at index 49 = 50ms, p95 = element at index 94 = 95ms.
	if snap[0].P50 != 50*time.Millisecond {
		t.Errorf("P50 = %v, want 50ms", snap[0].P50)
	}
	if snap[0].P95 != 95*time.Millisecond {
		t.Errorf("P95 = %v, want 95ms", snap[0].P95)
	}
}

func TestMetrics_PercentilesUnderfilled(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.record("q", 10*time.Millisecond, nil)
	m.record("q", 30*time.Millisecond, nil)

	snap := m.Snapshot()
	if snap[0].P50 == 0 || snap[0].P95 == 0 {
		t.Errorf("percentiles with 2 samples must be non-zero: %+v", snap[0])
	}
}
