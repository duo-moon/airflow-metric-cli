package model

import "time"

// DagRunState is the lifecycle state of a DAG run. Unknown values (added by
// future Airflow versions) are preserved verbatim so unfamiliar states
// still surface in the UI instead of being silently dropped.
type DagRunState string

// Well-known DagRunState values.
const (
	DagRunQueued  DagRunState = "queued"
	DagRunRunning DagRunState = "running"
	DagRunSuccess DagRunState = "success"
	DagRunFailed  DagRunState = "failed"
)

// IsTerminal reports whether the DAG run has reached a final state.
func (s DagRunState) IsTerminal() bool {
	return s == DagRunSuccess || s == DagRunFailed
}

// Dag is a static description of a DAG (does not change often). Kept as a
// forward-declaration for the day we start polling /dags — no collector
// populates it yet.
type Dag struct {
	ID               string
	IsPaused         bool
	IsActive         bool
	ScheduleInterval string
	Owner            string
	Tags             []string
	NextDagRun       time.Time
}

// DagRun is a single execution of a DAG.
type DagRun struct {
	DagID       string
	RunID       string
	State       DagRunState
	RunType     string
	LogicalDate time.Time
	Start       time.Time
	End         time.Time
	UpdatedAt   time.Time
	Note        string
}

// Duration returns the elapsed time between Start and End, or since Start for
// runs that are still going.
func (r DagRun) Duration() time.Duration {
	if r.Start.IsZero() {
		return 0
	}
	if r.End.IsZero() {
		return time.Since(r.Start)
	}
	return r.End.Sub(r.Start)
}
