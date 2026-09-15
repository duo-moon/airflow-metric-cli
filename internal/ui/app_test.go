package ui

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/store"
)

var ansi = regexp.MustCompile("\x1b\\[[0-9;]*[a-zA-Z]")

func stripANSI(s string) string { return ansi.ReplaceAllString(s, "") }

func newModelWith(t *testing.T, h model.ClusterHealth, now time.Time) Model {
	t.Helper()
	s := store.New()
	if len(h.Components) > 0 || !h.ObservedAt.IsZero() {
		s.UpsertHealth(h)
	}
	m := New(context.Background(), s, nil, "https://airflow.example.com", "0.0.0-test")
	m.now = now
	m.width, m.height = 80, 24
	return m
}

func TestView_EmptyState(t *testing.T) {
	t.Parallel()

	m := newModelWith(t, model.ClusterHealth{}, time.Now())
	out := stripANSI(m.View())

	for _, want := range []string{
		"afmetric",
		"https://airflow.example.com",
		"Cluster Health",
		"waiting for first poll",
		"q quit",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in view:\n%s", want, out)
		}
	}
}

func TestView_MixedHealth(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 18, 0, 0, 0, time.UTC)
	h := model.ClusterHealth{
		ObservedAt: now.Add(-3 * time.Second),
		Components: []model.ComponentHealth{
			{Name: "Scheduler", Status: model.HealthHealthy, LatestHeartbeat: now.Add(-12 * time.Second)},
			{Name: "Database", Status: model.HealthHealthy},
			{Name: "Triggerer", Status: model.HealthUnhealthy, LatestHeartbeat: now.Add(-4 * time.Minute)},
		},
	}
	m := newModelWith(t, h, now)
	out := stripANSI(m.View())

	for _, want := range []string{
		"Scheduler",
		"healthy",
		"last hb 12s ago",
		"Triggerer",
		"unhealthy",
		"last hb 4m ago",
		"observed 3s ago",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in view:\n%s", want, out)
		}
	}
}

func TestUpdate_QuitOnQ(t *testing.T) {
	t.Parallel()

	m := newModelWith(t, model.ClusterHealth{}, time.Now())
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	if cmd == nil {
		t.Fatal("q should emit tea.Quit, got nil")
	}
	if msg := cmd(); msg != tea.Quit() {
		t.Errorf("cmd = %T, want tea.Quit()", msg)
	}
}

func TestUpdate_WindowResize(t *testing.T) {
	t.Parallel()

	m := newModelWith(t, model.ClusterHealth{}, time.Now())
	newModel, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	got := newModel.(Model)
	if got.width != 120 || got.height != 40 {
		t.Errorf("size = %dx%d, want 120x40", got.width, got.height)
	}
}

func TestHumanizeDuration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		d    time.Duration
		want string
	}{
		{500 * time.Millisecond, "1s"}, // rounds up
		{45 * time.Second, "45s"},
		{2 * time.Minute, "2m"},
		{3 * time.Hour, "3h"},
		{-5 * time.Second, "0s"}, // negative clamped
	}
	for _, c := range cases {
		if got := humanizeDuration(c.d); got != c.want {
			t.Errorf("humanize(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}
