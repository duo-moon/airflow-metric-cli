// Package poller runs a set of named tasks on independent per-task intervals
// with optional jitter, records timing/error metrics and stops on ctx cancel.
package poller

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Task is a unit of work executed on a fixed interval.
//
//   - Fn receives the poller's ctx; return context.Canceled to indicate a
//     shutdown-induced abort (it will not be counted as an error).
//   - Jitter, if positive, spreads consecutive fires by ±Jitter/2 to avoid
//     synchronous bursts when several tasks share an interval. The delayed
//     value is floor-clamped at Interval/2 so a large jitter can't produce
//     a near-zero timer and a busy loop.
type Task struct {
	Name     string
	Interval time.Duration
	Jitter   time.Duration
	Fn       func(ctx context.Context) error
}

// Poller schedules a set of Tasks concurrently. Intervals are stored per-task
// as atomics so an out-of-band supervisor (see Guard) can dial them up/down
// at runtime.
type Poller struct {
	tasks     []Task
	metrics   *Metrics
	intervals sync.Map    // name -> *atomic.Int64 nanoseconds
	running   atomic.Bool // set on the first Run to reject late Add calls
}

// New returns an empty Poller.
func New() *Poller {
	return &Poller{metrics: NewMetrics()}
}

// Add registers a Task. Must be called before Run; calling it afterwards
// panics — mutating `tasks` while runTask goroutines are alive is a data
// race no matter how careful we are elsewhere.
func (p *Poller) Add(t Task) {
	if p.running.Load() {
		panic(fmt.Sprintf("poller: Add(%q) called after Run", t.Name))
	}
	p.tasks = append(p.tasks, t)
	var v atomic.Int64
	v.Store(int64(t.Interval))
	p.intervals.Store(t.Name, &v)
}

// SetInterval updates the polling interval of a registered task at runtime.
// Safe to call from any goroutine. Unknown names are silently ignored.
func (p *Poller) SetInterval(name string, d time.Duration) {
	if v, ok := p.intervals.Load(name); ok {
		v.(*atomic.Int64).Store(int64(d))
	}
}

// CurrentInterval reports the interval a task is currently scheduled with.
// Zero if unknown.
func (p *Poller) CurrentInterval(name string) time.Duration {
	if v, ok := p.intervals.Load(name); ok {
		return time.Duration(v.(*atomic.Int64).Load())
	}
	return 0
}

// Metrics returns the metric collector shared by all tasks.
func (p *Poller) Metrics() *Metrics { return p.metrics }

// Run blocks until ctx is cancelled or every task returns. The returned error
// is nil on graceful shutdown.
func (p *Poller) Run(ctx context.Context) error {
	if len(p.tasks) == 0 {
		return errors.New("poller: no tasks registered")
	}
	p.running.Store(true)

	var wg sync.WaitGroup
	for _, t := range p.tasks {
		wg.Add(1)
		go func(t Task) {
			defer wg.Done()
			p.runTask(ctx, t)
		}(t)
	}
	wg.Wait()
	return nil
}

func (p *Poller) runTask(ctx context.Context, t Task) {
	timer := time.NewTimer(0) // first tick fires immediately
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			start := time.Now()
			err := t.Fn(ctx)
			dur := time.Since(start)

			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return
			}
			p.metrics.record(t.Name, dur, err)

			cur := p.CurrentInterval(t.Name)
			if cur <= 0 {
				cur = t.Interval
			}
			timer.Reset(nextInterval(cur, t.Jitter))
		}
	}
}

// nextInterval returns interval ± jitter/2. Non-positive jitter yields the
// bare interval. Uses math/rand/v2's top-level generator, which is safe for
// concurrent use.
//
// Floor-clamped at interval/2 so that a jitter larger than interval can't
// collapse the wait to zero (which would spin the goroutine).
func nextInterval(interval, jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return interval
	}
	delta := time.Duration(rand.Int64N(int64(jitter))) - jitter/2
	next := interval + delta
	if floor := interval / 2; next < floor {
		next = floor
	}
	return next
}
