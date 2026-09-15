package poller

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPoller_SetIntervalRuntime(t *testing.T) {
	t.Parallel()

	p := New()
	p.Add(Task{
		Name:     "t",
		Interval: 5 * time.Millisecond,
		Fn:       func(_ context.Context) error { return nil },
	})
	if got := p.CurrentInterval("t"); got != 5*time.Millisecond {
		t.Fatalf("initial CurrentInterval = %v, want 5ms", got)
	}
	p.SetInterval("t", 42*time.Millisecond)
	if got := p.CurrentInterval("t"); got != 42*time.Millisecond {
		t.Errorf("post-set CurrentInterval = %v, want 42ms", got)
	}
	p.SetInterval("unknown", time.Second) // must not panic
}

func TestGuard_BacksOffOnErrors(t *testing.T) {
	t.Parallel()

	p := New()
	base := 100 * time.Millisecond
	p.Add(Task{Name: "flaky", Interval: base, Fn: func(_ context.Context) error { return nil }})

	// Seed metrics: 10 runs, 8 errors → rate 0.8 > 0.5.
	m := p.Metrics()
	for i := 0; i < 10; i++ {
		var err error
		if i < 8 {
			err = errors.New("boom")
		}
		m.record("flaky", time.Millisecond, err)
	}

	g := NewGuard(p, 1*time.Second, 0.5)
	g.Watch("flaky", base)
	g.evaluate()

	if got := p.CurrentInterval("flaky"); got != 2*base {
		t.Errorf("interval after 1st degraded tick = %v, want %v", got, 2*base)
	}

	// Simulate another window of errors — should double again to 4x base.
	for i := 0; i < 10; i++ {
		m.record("flaky", time.Millisecond, errors.New("still bad"))
	}
	g.evaluate()
	if got := p.CurrentInterval("flaky"); got != 4*base {
		t.Errorf("interval after 2nd tick = %v, want %v", got, 4*base)
	}
}

func TestGuard_RecoversToBase(t *testing.T) {
	t.Parallel()

	p := New()
	base := 100 * time.Millisecond
	p.Add(Task{Name: "t", Interval: base, Fn: func(_ context.Context) error { return nil }})

	// Push interval up manually as if it had been throttled.
	p.SetInterval("t", 8*base)

	g := NewGuard(p, 1*time.Second, 0.5)
	g.Watch("t", base)

	// Two clean windows should halve it each time.
	m := p.Metrics()
	for i := 0; i < 5; i++ {
		m.record("t", time.Millisecond, nil)
	}
	g.evaluate()
	if got := p.CurrentInterval("t"); got != 4*base {
		t.Errorf("after 1st clean tick = %v, want %v", got, 4*base)
	}

	for i := 0; i < 5; i++ {
		m.record("t", time.Millisecond, nil)
	}
	g.evaluate()
	if got := p.CurrentInterval("t"); got != 2*base {
		t.Errorf("after 2nd clean tick = %v, want %v", got, 2*base)
	}

	// Further clean ticks bring it all the way back to base, no lower.
	for step := 0; step < 5; step++ {
		for i := 0; i < 5; i++ {
			m.record("t", time.Millisecond, nil)
		}
		g.evaluate()
	}
	if got := p.CurrentInterval("t"); got != base {
		t.Errorf("recovery should clamp at base, got %v want %v", got, base)
	}
}

func TestGuard_ClampsAtMax(t *testing.T) {
	t.Parallel()

	p := New()
	base := 100 * time.Millisecond
	maxI := 300 * time.Millisecond // half-doublings only
	p.Add(Task{Name: "t", Interval: base, Fn: func(_ context.Context) error { return nil }})

	g := NewGuard(p, maxI, 0.5)
	g.Watch("t", base)

	m := p.Metrics()
	for tick := 0; tick < 5; tick++ {
		for i := 0; i < 10; i++ {
			m.record("t", time.Millisecond, errors.New("bad"))
		}
		g.evaluate()
	}
	if got := p.CurrentInterval("t"); got != maxI {
		t.Errorf("clamp = %v, want %v", got, maxI)
	}
}

func TestGuard_IgnoresUnknown(t *testing.T) {
	t.Parallel()

	p := New()
	g := NewGuard(p, time.Second, 0.5)
	g.Watch("nope", time.Second)
	g.evaluate() // must not panic even with zero registered tasks
}
