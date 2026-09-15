package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
)

// renderHealthPanel renders the cluster health panel at the given content
// width (excluding panel border). Callers pass a stable `now` so tests are
// deterministic.
func renderHealthPanel(h model.ClusterHealth, now time.Time, width int) string {
	title := styleTitle.Render("Cluster Health")

	if len(h.Components) == 0 {
		body := styleMuted.Render("waiting for first poll…")
		return stylePanel.Width(width).Render(title + "\n" + body)
	}

	lines := make([]string, 0, len(h.Components))
	for _, c := range h.Components {
		dot, statusStyle := statusIndicator(c.Status)
		heartbeat := "—"
		if !c.LatestHeartbeat.IsZero() {
			heartbeat = "last hb " + humanizeDuration(now.Sub(c.LatestHeartbeat)) + " ago"
		}
		lines = append(lines, fmt.Sprintf("%s  %-14s %s  %s",
			dot,
			c.Name,
			statusStyle.Render(padRight(string(c.Status), 10)),
			styleMuted.Render(heartbeat),
		))
	}

	obs := ""
	if !h.ObservedAt.IsZero() {
		obs = styleMuted.Render("observed " + humanizeDuration(now.Sub(h.ObservedAt)) + " ago")
	}

	body := strings.Join(lines, "\n")
	if obs != "" {
		body += "\n" + obs
	}
	return stylePanel.Width(width).Render(title + "\n" + body)
}

func statusIndicator(s model.HealthStatus) (string, lipgloss.Style) {
	switch s {
	case model.HealthHealthy:
		return styleHealthy.Render("●"), styleHealthy
	case model.HealthUnhealthy:
		return styleUnhealthy.Render("●"), styleUnhealthy
	default:
		return styleUnknown.Render("○"), styleUnknown
	}
}

func humanizeDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}
