package ui

import "github.com/mattn/go-runewidth"

// Treat East-Asian ambiguous-width glyphs (● ◔ ○ ▸ … █ ░) as 2 cells wide.
// Most monospace fonts render them that way, and lipgloss.Width / uniseg
// otherwise default to 1 — mis-padding table columns.
func init() {
	runewidth.DefaultCondition.EastAsianWidth = true
}
