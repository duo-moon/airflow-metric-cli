package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
)

type taskInstancesViewState struct {
	dagID   string
	runID   string
	loading bool
	errMsg  string
	items   []model.TaskInstance
	width   int
	cursor  int
}

func renderTaskInstancesPanel(s taskInstancesViewState) string {
	title := styleTitle.Render(fmt.Sprintf("Task Instances — %s / %s", s.dagID, truncateStr(s.runID, 40)))

	if s.loading {
		body := styleMuted.Render("loading…")
		return stylePanel.Width(s.width).Render(title + "\n" + body)
	}
	if s.errMsg != "" {
		body := styleError.Render("error: " + s.errMsg)
		return stylePanel.Width(s.width).Render(title + "\n" + body)
	}
	if len(s.items) == 0 {
		body := styleMuted.Render("no task instances")
		return stylePanel.Width(s.width).Render(title + "\n" + body)
	}

	lines := make([]string, 0, len(s.items)+2)
	lines = append(lines, "  "+styleMuted.Render(formatTIRow("TASK", "STATE", "TRY", "DURATION", "OPERATOR", "POOL")))
	for i, t := range s.items {
		rawState, stateStyle := taskStateLabel(t.State)
		row := formatTIRow(
			truncateStr(t.TaskID, 34),
			stateStyle.Render(padOrTruncateVisible(rawState, 14)),
			fmt.Sprintf("%d/%d", t.TryNumber, t.MaxTries),
			humanizeDuration(t.Duration),
			truncateStr(t.Operator, 20),
			truncateStr(t.Pool, 14),
		)
		if i == s.cursor {
			row = styleSelected.Render("▸ " + row)
		} else {
			row = "  " + row
		}
		lines = append(lines, row)
	}
	return stylePanel.Width(s.width).Render(title + "\n" + strings.Join(lines, "\n"))
}

func formatTIRow(task, state, try, duration, operator, pool string) string {
	return fmt.Sprintf("%s  %s  %s  %s  %s  %s",
		padOrTruncateVisible(task, 34),
		padOrTruncateVisible(state, 14),
		padOrTruncateVisible(try, 5),
		padOrTruncateVisible(duration, 10),
		padOrTruncateVisible(operator, 20),
		pool,
	)
}

// taskStateLabel maps a task instance state to a short (raw, style) pair.
// Long official states are compacted so they fit the STATE column in the
// panel without truncation; unknown/future states get a "? " prefix and
// let the column's truncation policy do its job.
func taskStateLabel(s model.TaskState) (string, lipgloss.Style) {
	switch s {
	case model.TaskSuccess:
		return "● success", styleHealthy
	case model.TaskRunning:
		return "● running", styleHealthy
	case model.TaskQueued:
		return "◔ queued", styleUnknown
	case model.TaskScheduled:
		return "◔ scheduled", styleUnknown
	case model.TaskDeferred:
		return "◔ deferred", styleUnknown
	case model.TaskUpForRetry:
		return "◔ retry", styleUnknown
	case model.TaskUpForReschedule:
		return "◔ resched", styleUnknown
	case model.TaskRestarting:
		return "◔ restart", styleUnknown
	case model.TaskRemoved:
		return "○ removed", styleMuted
	case model.TaskFailed:
		return "● failed", styleUnhealthy
	case model.TaskUpstreamFailed:
		return "● up_fail", styleUnhealthy
	case model.TaskSkipped:
		return "○ skipped", styleMuted
	default:
		return "? " + string(s), styleMuted
	}
}
