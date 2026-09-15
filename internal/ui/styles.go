package ui

import "github.com/charmbracelet/lipgloss"

var (
	styleHealthy   = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))  // green
	styleUnhealthy = lipgloss.NewStyle().Foreground(lipgloss.Color("196")) // red
	styleUnknown   = lipgloss.NewStyle().Foreground(lipgloss.Color("245")) // gray
	styleMuted     = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	styleHeader    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("39"))
	styleTitle     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("208"))
	stylePanel     = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("240")).
			Padding(0, 1)
	styleSelected = lipgloss.NewStyle().
			Foreground(lipgloss.Color("39")).
			Bold(true)
	styleError = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true)
)
