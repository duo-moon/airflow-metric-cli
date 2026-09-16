package model

import "time"

// TaskState is the lifecycle state of a single task instance. String-based
// so unknown values from future Airflow releases pass through untouched
// instead of collapsing into a generic "unknown".
type TaskState string

// Well-known TaskState values.
const (
	TaskSuccess         TaskState = "success"
	TaskRunning         TaskState = "running"
	TaskFailed          TaskState = "failed"
	TaskUpstreamFailed  TaskState = "upstream_failed"
	TaskSkipped         TaskState = "skipped"
	TaskUpForRetry      TaskState = "up_for_retry"
	TaskUpForReschedule TaskState = "up_for_reschedule"
	TaskQueued          TaskState = "queued"
	TaskScheduled       TaskState = "scheduled"
	TaskDeferred        TaskState = "deferred"
	TaskRestarting      TaskState = "restarting"
	TaskRemoved         TaskState = "removed"
)

// IsTerminal reports whether the task attempt has reached a final state.
// Note: up_for_retry is *not* terminal — Airflow will spawn another attempt.
func (s TaskState) IsTerminal() bool {
	switch s {
	case TaskSuccess, TaskFailed, TaskUpstreamFailed, TaskSkipped, TaskRemoved:
		return true
	}
	return false
}

// TaskInstance is a single attempt of a task in a DAG run.
type TaskInstance struct {
	DagID     string
	RunID     string
	TaskID    string
	TryNumber int
	MaxTries  int
	MapIndex  int
	State     TaskState
	Start     time.Time
	End       time.Time
	Duration  time.Duration
	Operator  string
	Executor  string
	Pool      string
	Queue     string
	Hostname  string
}

// TaskAttempt is one historical execution of a task instance. Airflow
// bumps TryNumber on retries *and* on manual clears, so a task with
// max_tries=3 can easily have TryNumber up to 8 or more if the user has
// been re-running it. Callers must not derive attempt counts from
// TaskInstance.MaxTries — read them from a list of TaskAttempt instead.
type TaskAttempt struct {
	TryNumber int
	State     TaskState
	Start     time.Time
	End       time.Time
}
