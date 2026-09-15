package ui

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
)

// Fetcher is the narrow REST surface the TUI needs for on-demand drill-down.
// It is implemented by client.AirflowClient — kept as a separate interface
// so tests can inject a fake without pulling in the whole client package.
type Fetcher interface {
	TaskInstances(ctx context.Context, dagID, runID string) ([]model.TaskInstance, error)
	TaskTries(ctx context.Context, dagID, runID, taskID string) ([]model.TaskAttempt, error)
	TaskLogs(ctx context.Context, dagID, runID, taskID string, tryNumber int) (string, error)
}

type taskInstancesLoadedMsg struct {
	dagID string
	runID string
	items []model.TaskInstance
}

type taskInstancesErrMsg struct {
	dagID string
	runID string
	err   error
}

// fetchTaskInstancesCmd returns a tea.Cmd that fetches task instances for a
// specific DAG run. Uses a 15s per-request timeout so a hanging Airflow does
// not wedge the UI.
func fetchTaskInstancesCmd(ctx context.Context, f Fetcher, dagID, runID string) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		items, err := f.TaskInstances(callCtx, dagID, runID)
		if err != nil {
			return taskInstancesErrMsg{dagID: dagID, runID: runID, err: err}
		}
		return taskInstancesLoadedMsg{dagID: dagID, runID: runID, items: items}
	}
}

type taskLogsLoadedMsg struct {
	dagID, runID, taskID string
	tryNumber            int
	content              string
}

type taskLogsErrMsg struct {
	dagID, runID, taskID string
	tryNumber            int
	err                  error
}

type taskTriesLoadedMsg struct {
	dagID, runID, taskID string
	attempts             []model.TaskAttempt
}

type taskTriesErrMsg struct {
	dagID, runID, taskID string
	err                  error
}

// fetchTaskTriesCmd returns a tea.Cmd that lists every historical attempt
// of a task instance. Runs alongside fetchTaskLogsCmd on Enter so we know
// the real range of try numbers, not just MaxTries.
func fetchTaskTriesCmd(ctx context.Context, f Fetcher, dagID, runID, taskID string) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		items, err := f.TaskTries(callCtx, dagID, runID, taskID)
		if err != nil {
			return taskTriesErrMsg{dagID: dagID, runID: runID, taskID: taskID, err: err}
		}
		return taskTriesLoadedMsg{dagID: dagID, runID: runID, taskID: taskID, attempts: items}
	}
}

// fetchTaskLogsCmd returns a tea.Cmd that fetches full logs for one attempt.
func fetchTaskLogsCmd(ctx context.Context, f Fetcher, dagID, runID, taskID string, tryNumber int) tea.Cmd {
	return func() tea.Msg {
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		content, err := f.TaskLogs(callCtx, dagID, runID, taskID, tryNumber)
		if err != nil {
			return taskLogsErrMsg{dagID: dagID, runID: runID, taskID: taskID, tryNumber: tryNumber, err: err}
		}
		return taskLogsLoadedMsg{dagID: dagID, runID: runID, taskID: taskID, tryNumber: tryNumber, content: content}
	}
}
