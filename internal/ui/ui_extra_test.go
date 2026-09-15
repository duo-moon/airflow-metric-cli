package ui

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/store"
)

// --- log-search input mode -----------------------------------------------

func TestHandleLogSearchInput_CommitEnterJumpsToMatch(t *testing.T) {
	t.Parallel()

	// 12 lines total, viewport only 3 lines — first ERROR sits at index 5
	// so committing the pattern must scroll the viewport down to it.
	lines := []string{
		"noise 0", "noise 1", "noise 2", "noise 3", "noise 4",
		"ERROR — first hit at line 5",
		"noise 6", "noise 7",
		"ERROR — second at line 8",
		"noise 9", "noise 10", "noise 11",
	}
	m := Model{
		logContent:     strings.Join(lines, "\n"),
		logHeight:      3,
		logSearchMode:  true,
		logSearchInput: "error",
	}
	next, _ := m.handleLogSearchInput(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)

	if got.logSearchMode {
		t.Errorf("Enter must exit search mode")
	}
	if got.logSearchActive != "error" {
		t.Errorf("committed pattern = %q, want %q", got.logSearchActive, "error")
	}
	if len(got.logSearchMatches) != 2 {
		t.Fatalf("matches = %d, want 2", len(got.logSearchMatches))
	}
	if got.logSearchIdx != 0 {
		t.Errorf("cursor at match index = %d, want 0", got.logSearchIdx)
	}
	if got.logOffset != 5 {
		t.Errorf("viewport didn't jump to first match: offset=%d want 5", got.logOffset)
	}
}

func TestHandleLogSearchInput_EscAborts(t *testing.T) {
	t.Parallel()

	m := Model{
		logSearchMode:   true,
		logSearchInput:  "partial",
		logSearchActive: "previous",
	}
	next, _ := m.handleLogSearchInput(tea.KeyMsg{Type: tea.KeyEsc})
	got := next.(Model)

	if got.logSearchMode || got.logSearchInput != "" {
		t.Errorf("Esc must clear input mode + input: %+v", got)
	}
	if got.logSearchActive != "previous" {
		t.Errorf("Esc must NOT touch the already-committed pattern, got %q", got.logSearchActive)
	}
}

func TestHandleLogSearchInput_BackspaceAndRunes(t *testing.T) {
	t.Parallel()

	m := Model{logSearchMode: true, logSearchInput: "err"}

	// Backspace removes trailing character.
	next, _ := m.handleLogSearchInput(tea.KeyMsg{Type: tea.KeyBackspace})
	got := next.(Model)
	if got.logSearchInput != "er" {
		t.Errorf("after Backspace, input = %q, want %q", got.logSearchInput, "er")
	}

	// Runes append.
	next2, _ := got.handleLogSearchInput(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	got2 := next2.(Model)
	if got2.logSearchInput != "err" {
		t.Errorf("after append, input = %q, want %q", got2.logSearchInput, "err")
	}
	if !got2.logSearchMode {
		t.Errorf("still expected to be in search mode during typing")
	}
}

// --- transitions ----------------------------------------------------------

func TestTransition_TaskInstances_Enter_OpensLogs(t *testing.T) {
	t.Parallel()

	s := store.New()
	fetcher := &fakeFetcher{}
	m := New(context.Background(), s, fetcher, "http://x", "0")
	m.width, m.height = 120, 30
	m.screen = screenTaskInstances
	m.drillDagID = "d"
	m.drillRunID = "r"
	m.drillItems = []model.TaskInstance{
		{DagID: "d", RunID: "r", TaskID: "extract", TryNumber: 3, MaxTries: 5, State: model.TaskFailed},
	}
	m.drillCursor = 0

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	got := next.(Model)

	if got.screen != screenTaskLogs {
		t.Fatalf("screen = %v, want screenTaskLogs", got.screen)
	}
	if got.logDagID != "d" || got.logRunID != "r" || got.logTaskID != "extract" {
		t.Errorf("log target wrong: %s/%s/%s", got.logDagID, got.logRunID, got.logTaskID)
	}
	if got.logTryNumber != 3 {
		t.Errorf("logTryNumber = %d, want 3 (from ti.TryNumber)", got.logTryNumber)
	}
	if !got.logLoading {
		t.Errorf("logLoading must be true immediately after enter")
	}
	if cmd == nil {
		t.Error("Enter must return a tea.Cmd batch (logs + tries)")
	}
}

// --- layout ---------------------------------------------------------------

func TestView_NarrowLayoutStacksVertically(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s := store.New()
	s.UpsertDagRuns([]model.DagRun{
		{DagID: "a", RunID: "r1", State: model.DagRunRunning, Start: now.Add(-time.Minute), UpdatedAt: now},
		{DagID: "b", RunID: "r2", State: model.DagRunFailed, Start: now.Add(-2 * time.Minute), End: now.Add(-30 * time.Second), UpdatedAt: now},
	})
	m := New(context.Background(), s, nil, "http://x", "0")
	m.now = now
	// Narrow terminal — below wideLayoutMinWidth (100). Panels must stack.
	m.width, m.height = 80, 40

	out := stripANSI(m.View())
	// Locate row indices of the two panel titles. A wide layout would
	// print them on the *same* row; narrow layout keeps them on different
	// rows because JoinVertical is used.
	lines := strings.Split(out, "\n")
	activeRow, failuresRow := -1, -1
	for i, l := range lines {
		if strings.Contains(l, "Active Runs (1)") {
			activeRow = i
		}
		if strings.Contains(l, "Recent Failures (1)") {
			failuresRow = i
		}
	}
	if activeRow < 0 || failuresRow < 0 {
		t.Fatalf("could not locate panel titles; output:\n%s", out)
	}
	if activeRow == failuresRow {
		t.Errorf("narrow layout must stack panels (got both titles on row %d)", activeRow)
	}
}
