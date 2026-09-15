package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
)

const failuresMax = 8

func renderFailuresPanel(runs []model.DagRun, now time.Time, width int, focused bool, cursor int) string {
	title := renderPanelTitle("Recent Failures", len(runs), focused)

	if len(runs) == 0 {
		body := styleMuted.Render("no failures in the last hour")
		return stylePanel.Width(width).Render(title + "\n" + body)
	}

	start, end := scrollWindow(len(runs), cursor, failuresMax)

	lines := make([]string, 0, end-start+3)
	lines = append(lines, "  "+styleMuted.Render(formatFailureRow("DAG", "RUN", "FAILED", "DURATION")))
	if start > 0 {
		lines = append(lines, styleMuted.Render(fmt.Sprintf("  … %d above", start)))
	}
	for i := start; i < end; i++ {
		r := runs[i]
		row := styleUnhealthy.Render(formatFailureRow(
			truncateStr(r.DagID, 30),
			truncateStr(r.RunID, 25),
			formatRelative(r.End, now),
			humanizeDuration(r.Duration()),
		))
		if focused && i == cursor {
			row = styleSelected.Render("▸ " + row)
		} else {
			row = "  " + row
		}
		lines = append(lines, row)
	}
	if end < len(runs) {
		lines = append(lines, styleMuted.Render(fmt.Sprintf("  … %d below", len(runs)-end)))
	}

	return stylePanel.Width(width).Render(title + "\n" + strings.Join(lines, "\n"))
}

func formatFailureRow(dag, run, failed, duration string) string {
	return fmt.Sprintf("%s  %s  %s  %s",
		padOrTruncateVisible(dag, 30),
		padOrTruncateVisible(run, 25),
		padOrTruncateVisible(failed, 12),
		duration,
	)
}
