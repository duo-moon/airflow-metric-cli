package model

// WaitingCount aggregates non-terminal, non-running task instance counts for
// one DAG run. Zero-value counts are represented as literal zeros — the
// panels differentiate presence vs. absence via the map lookup, not the
// count itself.
type WaitingCount struct {
	DagID      string
	RunID      string
	Reschedule int // up_for_reschedule
	Retry      int // up_for_retry
	Scheduled  int
	Queued     int
	Deferred   int
}

// Total returns the sum of all waiting states.
func (w WaitingCount) Total() int {
	return w.Reschedule + w.Retry + w.Scheduled + w.Queued + w.Deferred
}
