package ui

import (
	"fmt"
	"strings"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
)

const (
	poolBarWidth = 20
	poolBarFull  = "█"
	poolBarEmpty = "░"
)

func renderPoolsPanel(pools []model.Pool, width int) string {
	title := styleTitle.Render(fmt.Sprintf("Pools (%d)", len(pools)))

	if len(pools) == 0 {
		body := styleMuted.Render("no pools reported")
		return stylePanel.Width(width).Render(title + "\n" + body)
	}

	lines := make([]string, 0, len(pools)+1)
	lines = append(lines, styleMuted.Render(formatPoolRow("NAME", "SLOTS", "USAGE", "BAR")))
	for _, p := range pools {
		slots := fmt.Sprintf("%d/%d", p.OccupiedSlots, p.Slots)
		usage := fmt.Sprintf("%3.0f%%", p.Utilization()*100)
		bar := renderPoolBar(p.Utilization())
		lines = append(lines, formatPoolRow(
			truncateStr(p.Name, 24),
			slots,
			usage,
			bar,
		))
	}
	return stylePanel.Width(width).Render(title + "\n" + strings.Join(lines, "\n"))
}

func formatPoolRow(name, slots, usage, bar string) string {
	return fmt.Sprintf("%s  %s  %s  %s",
		padRight(name, 24),
		padRight(slots, 10),
		padRight(usage, 5),
		bar,
	)
}

// renderPoolBar draws a fixed-width bar of poolBarWidth. Colour changes with
// utilisation: green (<70%), yellow (<90%), red (≥90%).
func renderPoolBar(u float64) string {
	if u < 0 {
		u = 0
	}
	if u > 1 {
		u = 1
	}
	filled := int(float64(poolBarWidth) * u)
	if u > 0 && filled == 0 {
		filled = 1
	}
	bar := strings.Repeat(poolBarFull, filled) + strings.Repeat(poolBarEmpty, poolBarWidth-filled)

	switch {
	case u >= 0.9:
		return styleUnhealthy.Render(bar)
	case u >= 0.7:
		return styleUnknown.Render(bar)
	default:
		return styleHealthy.Render(bar)
	}
}
