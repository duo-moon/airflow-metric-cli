package ui

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/store"
)

// fakeFetcher lets tests drive drill-down without hitting the network.
type fakeFetcher struct {
	items []model.TaskInstance
	err   error
	calls int
}

func (f *fakeFetcher) TaskInstances(_ context.Context, _, _ string) ([]model.TaskInstance, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.items, nil
}

func (f *fakeFetcher) TaskLogs(_ context.Context, _, _, _ string, _ int) (string, error) {
	return "", nil
}

func (f *fakeFetcher) TaskTries(_ context.Context, _, _, _ string) ([]model.TaskAttempt, error) {
	return nil, nil
}

func seedRuns(t *testing.T) *store.Store {
	t.Helper()
	now := time.Date(2026, 8, 17, 18, 0, 0, 0, time.UTC)
	s := store.New()
	s.UpsertDagRuns([]model.DagRun{
		{DagID: "etl_daily", RunID: "run-a", State: model.DagRunRunning, Start: now.Add(-2 * time.Minute), UpdatedAt: now},
		{DagID: "reports", RunID: "run-b", State: model.DagRunQueued, Start: now.Add(-30 * time.Second), UpdatedAt: now},
		{DagID: "cleanup", RunID: "run-c", State: model.DagRunFailed, Start: now.Add(-10 * time.Minute), End: now.Add(-5 * time.Minute), UpdatedAt: now},
	})
	return s
}

func TestNavigation_TabAndCursor(t *testing.T) {
	t.Parallel()

	s := seedRuns(t)
	m := New(context.Background(), s, nil, "http://x", "0")
	m.width, m.height = 120, 30

	m1, _ := m.Update(keyDown())
	m1, _ = m1.(Model).Update(keyDown())
	if got := m1.(Model).cursor; got != 1 {
		t.Errorf("cursor after 2× down = %d, want 1 (clamped to len-1=1 for 2 active runs)", got)
	}

	m2, _ := m1.(Model).Update(keyTab())
	got := m2.(Model)
	if got.focus != focusFailures {
		t.Errorf("tab should move focus to failures, got %v", got.focus)
	}
	if got.cursor != 0 {
		t.Errorf("cursor must reset on tab, got %d", got.cursor)
	}

	m3, _ := got.Update(keyUp()) // already at 0, must clamp
	if got := m3.(Model).cursor; got != 0 {
		t.Errorf("cursor should clamp at 0, got %d", got)
	}
}

func TestNavigation_EnterOpensDrill(t *testing.T) {
	t.Parallel()

	s := seedRuns(t)
	fetcher := &fakeFetcher{items: []model.TaskInstance{
		{DagID: "etl_daily", RunID: "run-a", TaskID: "extract", TryNumber: 1, MaxTries: 3, State: "running", Duration: 12 * time.Second, Operator: "PythonOperator"},
	}}
	m := New(context.Background(), s, fetcher, "http://x", "0")
	m.width, m.height = 120, 30

	m2, cmd := m.Update(keyEnter())
	got := m2.(Model)
	if got.screen != screenTaskInstances {
		t.Fatalf("screen = %v, want screenTaskInstances", got.screen)
	}
	if got.drillDagID != "etl_daily" || got.drillRunID != "run-a" {
		t.Errorf("drill target = %s/%s, want etl_daily/run-a", got.drillDagID, got.drillRunID)
	}
	if !got.drillLoading {
		t.Error("drillLoading should be true immediately after Enter")
	}
	if cmd == nil {
		t.Fatal("Enter must return a fetch Cmd")
	}

	// Execute the Cmd synchronously and feed the msg back.
	msg := cmd()
	m3, _ := got.Update(msg)
	loaded := m3.(Model)
	if loaded.drillLoading {
		t.Error("drillLoading should be false after fetch")
	}
	if len(loaded.drillItems) != 1 {
		t.Fatalf("drillItems len = %d, want 1", len(loaded.drillItems))
	}
	if fetcher.calls != 1 {
		t.Errorf("fetcher.calls = %d, want 1", fetcher.calls)
	}
}

func TestNavigation_FetchError(t *testing.T) {
	t.Parallel()

	s := seedRuns(t)
	fetcher := &fakeFetcher{err: errors.New("HTTP 500")}
	m := New(context.Background(), s, fetcher, "http://x", "0")

	m2, cmd := m.Update(keyEnter())
	msg := cmd()
	m3, _ := m2.(Model).Update(msg)
	got := m3.(Model)
	if got.drillErr == "" || !strings.Contains(got.drillErr, "500") {
		t.Errorf("drillErr = %q, want to contain 500", got.drillErr)
	}
	if got.drillLoading {
		t.Error("drillLoading should be false after error")
	}
}

func TestNavigation_EscReturnsToDashboard(t *testing.T) {
	t.Parallel()

	s := seedRuns(t)
	fetcher := &fakeFetcher{}
	m := New(context.Background(), s, fetcher, "http://x", "0")
	m.screen = screenTaskInstances
	m.drillDagID = "etl_daily"
	m.drillRunID = "run-a"

	m2, _ := m.Update(tea.KeyMsg{Type: tea.KeyEscape})
	if m2.(Model).screen != screenDashboard {
		t.Errorf("esc must return to dashboard, got %v", m2.(Model).screen)
	}
}

func TestView_TaskInstancesLoadingAndData(t *testing.T) {
	t.Parallel()

	m := Model{
		store:        store.New(),
		width:        100,
		screen:       screenTaskInstances,
		drillDagID:   "etl",
		drillRunID:   "run-x",
		drillLoading: true,
	}
	out := stripANSI(m.View())
	if !strings.Contains(out, "loading") {
		t.Errorf("loading state missing:\n%s", out)
	}

	m.drillLoading = false
	m.drillItems = []model.TaskInstance{
		{TaskID: "extract", State: "success", TryNumber: 1, MaxTries: 3, Duration: 12 * time.Second, Operator: "PythonOperator"},
		{TaskID: "transform", State: "failed", TryNumber: 2, MaxTries: 3, Duration: 30 * time.Second, Operator: "BashOperator"},
	}
	out = stripANSI(m.View())
	for _, want := range []string{"extract", "success", "transform", "failed", "TASK", "TRY", "DURATION"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
}

// Helpers to build tea.KeyMsg without pulling in the whole key package.
func keyEnter() tea.KeyMsg { return tea.KeyMsg{Type: tea.KeyEnter} }
func keyTab() tea.KeyMsg   { return tea.KeyMsg{Type: tea.KeyTab} }
func keyDown() tea.KeyMsg  { return tea.KeyMsg{Type: tea.KeyDown} }
func keyUp() tea.KeyMsg    { return tea.KeyMsg{Type: tea.KeyUp} }
