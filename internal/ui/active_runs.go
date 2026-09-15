package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-runewidth"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
)

const activeRunsMax = 8

// waitingLookup returns the aggregated waiting count for a specific run.
// Passed as a function so the panel doesn't need to know about *store.Store.
type waitingLookup func(dagID, runID string) model.WaitingCount

func renderActiveRunsPanel(runs []model.DagRun, waiting waitingLookup, now time.Time, width int, focused bool, cursor int) string {
	title := renderPanelTitle("Active Runs", len(runs), focused)

	if len(runs) == 0 {
		body := styleMuted.Render("no active DAG runs")
		return stylePanel.Width(width).Render(title + "\n" + body)
	}

	start, end := scrollWindow(len(runs), cursor, activeRunsMax)

	lines := make([]string, 0, end-start+3)
	// "  " matches the cursor-slot prefix on data rows so columns align.
	lines = append(lines, "  "+styleMuted.Render(formatRunRow("DAG", "RUN", "STATE", "WAIT", "STARTED", "DURATION")))
	if start > 0 {
		lines = append(lines, styleMuted.Render(fmt.Sprintf("  … %d above", start)))
	}
	for i := start; i < end; i++ {
		r := runs[i]
		rawState, stateStyle := runStateLabel(r.State)
		row := formatRunRow(
			truncateStr(r.DagID, 30),
			truncateStr(r.RunID, 25),
			stateStyle.Render(padOrTruncateVisible(rawState, 12)),
			formatWaitCell(waiting, r),
			formatRelative(r.Start, now),
			humanizeDuration(r.Duration()),
		)
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

func formatRunRow(dag, run, state, wait, started, duration string) string {
	return fmt.Sprintf("%s  %s  %s  %s  %s  %s",
		padOrTruncateVisible(dag, 30),
		padOrTruncateVisible(run, 25),
		padOrTruncateVisible(state, 12),
		padOrTruncateVisible(wait, 4),
		padOrTruncateVisible(started, 10),
		duration,
	)
}

// formatWaitCell renders the compact WAIT column. Zero renders as a muted
// dash so a healthy row stays visually quiet.
func formatWaitCell(lookup waitingLookup, r model.DagRun) string {
	if lookup == nil {
		return styleMuted.Render("-")
	}
	w := lookup(r.DagID, r.RunID)
	total := w.Total()
	if total == 0 {
		return styleMuted.Render("-")
	}
	return styleUnknown.Render(fmt.Sprintf("%d", total))
}

// runStateLabel returns (raw, style) for a DagRunState. Raw string stays
// short enough to fit the STATE column without truncation for every state
// the API is known to emit; unknown states are prefixed with '?' and let
// the column's truncation policy handle overflow.
func runStateLabel(s model.DagRunState) (string, lipgloss.Style) {
	switch s {
	case model.DagRunRunning:
		return "● running", styleHealthy
	case model.DagRunQueued:
		return "◔ queued", styleUnknown
	case model.DagRunFailed:
		return "● failed", styleUnhealthy
	case model.DagRunSuccess:
		return "● success", styleHealthy
	default:
		return "? " + string(s), styleMuted
	}
}

func formatRelative(t, now time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := now.Sub(t)
	if d < 0 {
		return "in " + humanizeDuration(-d)
	}
	return humanizeDuration(d) + " ago"
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n < 1 {
		return ""
	}
	return s[:n-1] + "…"
}

// padOrTruncateVisible enforces an exact visible width of n cells: pads
// shorter strings with spaces, and truncates longer ones rune-by-rune so
// columns never drift.
//
// Truncation is safe for plain text (no ANSI). Callers that need to style
// the result should style the value returned here, not the other way
// around — styling first and truncating second would slice through escape
// sequences.
func padOrTruncateVisible(s string, n int) string {
	w := lipgloss.Width(s)
	if w == n {
		return s
	}
	if w < n {
		return s + strings.Repeat(" ", n-w)
	}
	var b strings.Builder
	cur := 0
	for _, r := range s {
		rw := runewidth.RuneWidth(r)
		if cur+rw > n {
			break
		}
		b.WriteRune(r)
		cur += rw
	}
	if cur < n {
		return b.String() + strings.Repeat(" ", n-cur)
	}
	return b.String()
}

// renderPanelTitle formats "Title (N)" and marks the focused panel with a
// leading arrow so users can tell where the keyboard is aimed.
func renderPanelTitle(name string, count int, focused bool) string {
	prefix := "  "
	if focused {
		prefix = styleTitle.Render("▸ ")
	}
	return prefix + styleTitle.Render(fmt.Sprintf("%s (%d)", name, count))
}

// scrollWindow returns the [start, end) row slice to display so that cursor
// stays visible within a viewport of the given size. Anchors the cursor to
// the last visible row once we've scrolled past capacity.
func scrollWindow(total, cursor, size int) (int, int) {
	if total <= size {
		return 0, total
	}
	start := 0
	if cursor >= size {
		start = cursor - size + 1
	}
	if start > total-size {
		start = total - size
	}
	if start < 0 {
		start = 0
	}
	end := start + size
	if end > total {
		end = total
	}
	return start, end
}
