package client

import (
	"context"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
)

// AirflowClient is the API-version-independent surface the collectors and
// the TUI Fetcher talk to. Each concrete adapter (one per REST API dialect —
// see APIVersion) owns its own OpenAPI-generated client and DTO→model
// mapping, so callers never see raw REST types.
//
// All methods must be safe for concurrent use.
type AirflowClient interface {
	// Version returns the running Airflow version and its git ref.
	Version(ctx context.Context) (version, gitVersion string, err error)

	// Health returns the current cluster component status.
	Health(ctx context.Context) (model.ClusterHealth, error)

	// DagRuns returns DAG runs filtered by state and capped at pageLimit,
	// ordered by -start_date so the freshest runs come first.
	DagRuns(ctx context.Context, states []model.DagRunState, pageLimit int) ([]model.DagRun, error)

	// Pools returns all slot pools.
	Pools(ctx context.Context) ([]model.Pool, error)

	// ImportErrors returns DAG parse errors.
	ImportErrors(ctx context.Context) ([]model.ImportError, error)

	// WaitingTasks returns aggregated per-(dag,run) counts of task instances
	// in waiting states (queued / scheduled / up_for_retry / up_for_reschedule /
	// deferred).
	WaitingTasks(ctx context.Context, pageLimit int) ([]model.WaitingCount, error)

	// TaskInstances returns task instances of a specific DAG run.
	TaskInstances(ctx context.Context, dagID, runID string) ([]model.TaskInstance, error)

	// TaskTries returns every historical attempt of one task instance,
	// ordered by TryNumber ascending. Because Airflow bumps try_number on
	// manual clears in addition to retries, this is the only reliable way
	// to enumerate available log-file numbers.
	TaskTries(ctx context.Context, dagID, runID, taskID string) ([]model.TaskAttempt, error)

	// TaskLogs returns the full log content for a specific task attempt.
	TaskLogs(ctx context.Context, dagID, runID, taskID string, tryNumber int) (string, error)
}
