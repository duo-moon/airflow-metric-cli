package poller

import (
	"context"
	"time"
)

// Guard is a rate-limit supervisor. It periodically inspects task metrics
// and dials each registered task's interval up on errors (exponential
// backoff, capped at MaxInterval) and back down when errors clear.
//
// Guard itself is registered as a Task through Guard.Task(). It never
// modifies its own interval.
//
// Concurrency: Watch is meant to run at setup time, before Guard.Task is
// registered with the Poller. evaluate then executes exclusively inside
// the Poller goroutine dedicated to the guard task, so `base` and `prev`
// need no locks — they have a single reader and a single writer, and the
// writer is the same goroutine that owns the maps. If a second entry
// point to evaluate ever appears, this invariant must be revisited.
type Guard struct {
	poller      *Poller
	maxInterval time.Duration
	threshold   float64 // error fraction that triggers backoff, e.g. 0.5

	// Static per-task config supplied at Watch().
	base map[string]time.Duration

	// Rolling deltas — remembered across ticks.
	prev map[string]TaskStats
}

// NewGuard creates a Guard bound to a Poller. errorThreshold is the fraction
// of failed runs (in one window between ticks) that triggers a doubling of
// the interval; a full clean window halves it back to base.
func NewGuard(p *Poller, maxInterval time.Duration, errorThreshold float64) *Guard {
	if errorThreshold <= 0 {
		errorThreshold = 0.5
	}
	if maxInterval <= 0 {
		maxInterval = 5 * time.Minute
	}
	return &Guard{
		poller:      p,
		maxInterval: maxInterval,
		threshold:   errorThreshold,
		base:        make(map[string]time.Duration),
		prev:        make(map[string]TaskStats),
	}
}

// Watch registers a task by name with its baseline interval. The poller
// task with the same name must already be registered.
func (g *Guard) Watch(name string, base time.Duration) {
	g.base[name] = base
}

// Task returns a poller.Task suitable for registration alongside collectors.
// The tick interval controls how often adaptation happens; a value in the
// range 15s–1m is a good default.
func (g *Guard) Task(tickInterval time.Duration) Task {
	return Task{
		Name:     "guard",
		Interval: tickInterval,
		Fn: func(_ context.Context) error {
			g.evaluate()
			return nil
		},
	}
}

// evaluate reads the poller's metrics and adjusts intervals in-place.
func (g *Guard) evaluate() {
	snap := g.poller.Metrics().Snapshot()
	byName := make(map[string]TaskStats, len(snap))
	for _, s := range snap {
		byName[s.Name] = s
	}

	for name, base := range g.base {
		cur, ok := byName[name]
		if !ok {
			continue
		}
		prev := g.prev[name]

		runs := cur.Runs - prev.Runs
		errs := cur.Errors - prev.Errors
		g.prev[name] = cur

		if runs == 0 {
			continue
		}
		rate := float64(errs) / float64(runs)

		curInterval := g.poller.CurrentInterval(name)
		if curInterval <= 0 {
			curInterval = base
		}

		switch {
		case rate >= g.threshold:
			next := curInterval * 2
			if next > g.maxInterval {
				next = g.maxInterval
			}
			if next != curInterval {
				g.poller.SetInterval(name, next)
			}
		case errs == 0 && curInterval > base:
			next := curInterval / 2
			if next < base {
				next = base
			}
			g.poller.SetInterval(name, next)
		}
	}
}
