package poller

import (
	"slices"
	"sync"
	"time"
)

// latencySampleSize caps the ring buffer used for percentile estimation.
// 128 samples give a stable p95 while staying trivially cheap to sort.
const latencySampleSize = 128

// TaskStats is an immutable snapshot of a task's counters and latency
// percentiles at a point in time.
type TaskStats struct {
	Name      string
	Runs      uint64
	Errors    uint64
	LastRun   time.Time
	LastError string
	P50       time.Duration
	P95       time.Duration
}

// Metrics accumulates per-task counters and latency samples.
type Metrics struct {
	mu   sync.RWMutex
	data map[string]*taskState
}

type taskState struct {
	mu      sync.Mutex
	runs    uint64
	errors  uint64
	last    time.Time
	lastErr string
	samples []time.Duration // capped at latencySampleSize, treated as a ring
	idx     int
	full    bool
}

// NewMetrics returns an empty Metrics.
func NewMetrics() *Metrics {
	return &Metrics{data: make(map[string]*taskState)}
}

// record captures a single task invocation.
func (m *Metrics) record(name string, dur time.Duration, err error) {
	st := m.stateFor(name)

	st.mu.Lock()
	defer st.mu.Unlock()

	st.runs++
	st.last = time.Now()
	if err != nil {
		st.errors++
		st.lastErr = err.Error()
	}

	if st.samples == nil {
		st.samples = make([]time.Duration, latencySampleSize)
	}
	st.samples[st.idx] = dur
	st.idx = (st.idx + 1) % latencySampleSize
	if st.idx == 0 {
		st.full = true
	}
}

func (m *Metrics) stateFor(name string) *taskState {
	m.mu.RLock()
	st, ok := m.data[name]
	m.mu.RUnlock()
	if ok {
		return st
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if st, ok = m.data[name]; ok {
		return st
	}
	st = &taskState{}
	m.data[name] = st
	return st
}

// Snapshot returns a stable copy of the current stats for every task ever
// recorded. Safe for concurrent use with record.
func (m *Metrics) Snapshot() []TaskStats {
	m.mu.RLock()
	names := make([]string, 0, len(m.data))
	states := make([]*taskState, 0, len(m.data))
	for name, st := range m.data {
		names = append(names, name)
		states = append(states, st)
	}
	m.mu.RUnlock()

	out := make([]TaskStats, len(names))
	for i, st := range states {
		st.mu.Lock()
		p50, p95 := percentiles(st.samples, st.idx, st.full)
		out[i] = TaskStats{
			Name:      names[i],
			Runs:      st.runs,
			Errors:    st.errors,
			LastRun:   st.last,
			LastError: st.lastErr,
			P50:       p50,
			P95:       p95,
		}
		st.mu.Unlock()
	}
	slices.SortFunc(out, func(a, b TaskStats) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		default:
			return 0
		}
	})
	return out
}

// percentiles returns (p50, p95) over the currently populated portion of the
// ring. Returns zero durations when no samples have been recorded.
func percentiles(ring []time.Duration, idx int, full bool) (time.Duration, time.Duration) {
	n := len(ring)
	if !full {
		n = idx
	}
	if n == 0 {
		return 0, 0
	}

	buf := make([]time.Duration, n)
	copy(buf, ring[:n])
	slices.Sort(buf)

	return quantile(buf, 0.50), quantile(buf, 0.95)
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	return sorted[idx]
}
