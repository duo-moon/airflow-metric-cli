package ui

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/store"
)

func TestRenderActiveRunsPanel_TableAndEmpty(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 18, 0, 0, 0, time.UTC)

	empty := stripANSI(renderActiveRunsPanel(nil, nil, now, 80, false, 0))
	if !strings.Contains(empty, "no active DAG runs") {
		t.Errorf("empty state missing:\n%s", empty)
	}
	if !strings.Contains(empty, "Active Runs (0)") {
		t.Errorf("title should show count 0:\n%s", empty)
	}

	runs := []model.DagRun{
		{DagID: "etl_daily", RunID: "scheduled__2026-08-17", State: model.DagRunRunning, Start: now.Add(-5 * time.Minute)},
		{DagID: "reports", RunID: "manual__001", State: model.DagRunQueued, Start: now.Add(-30 * time.Second)},
	}
	out := stripANSI(renderActiveRunsPanel(runs, nil, now, 100, false, 0))

	for _, want := range []string{
		"Active Runs (2)",
		"etl_daily",
		"scheduled__2026-08-17",
		"running",
		"5m ago",
		"reports",
		"queued",
		"30s ago",
		"DAG", "STATE", "STARTED", "DURATION",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in panel:\n%s", want, out)
		}
	}
}

func TestRenderActiveRunsPanel_Overflow(t *testing.T) {
	t.Parallel()

	now := time.Now()
	runs := make([]model.DagRun, activeRunsMax+3)
	for i := range runs {
		runs[i] = model.DagRun{DagID: "d", RunID: "r", State: model.DagRunRunning, Start: now.Add(-time.Minute)}
	}
	out := stripANSI(renderActiveRunsPanel(runs, nil, now, 100, false, 0))
	if !strings.Contains(out, "… 3 below") {
		t.Errorf("expected overflow indicator, got:\n%s", out)
	}
}

func TestPadOrTruncateVisible(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"abc", 5, "abc  "},                // pad
		{"abcdef", 5, "abcde"},             // truncate
		{"abcde", 5, "abcde"},              // exact
		{"● run", 5, "● run"},              // wide-char, fits exactly (2+1+3=6? actually depends)
		{"upstream_failed", 8, "upstream"}, // truncate to 8 ASCII cells exactly
	}
	for _, c := range cases {
		got := padOrTruncateVisible(c.in, c.n)
		if got != c.want {
			// Print the visible widths so failures are self-diagnostic.
			t.Errorf("padOrTruncateVisible(%q, %d) = %q (want %q)", c.in, c.n, got, c.want)
		}
	}
}

func TestScrollWindow(t *testing.T) {
	t.Parallel()

	cases := []struct {
		total, cursor, size int
		wantStart, wantEnd  int
	}{
		{total: 5, cursor: 0, size: 8, wantStart: 0, wantEnd: 5},     // fits fully
		{total: 20, cursor: 0, size: 8, wantStart: 0, wantEnd: 8},    // top of window
		{total: 20, cursor: 3, size: 8, wantStart: 0, wantEnd: 8},    // still no scroll
		{total: 20, cursor: 7, size: 8, wantStart: 0, wantEnd: 8},    // last visible row
		{total: 20, cursor: 8, size: 8, wantStart: 1, wantEnd: 9},    // scroll by 1
		{total: 20, cursor: 15, size: 8, wantStart: 8, wantEnd: 16},  // deeper
		{total: 20, cursor: 19, size: 8, wantStart: 12, wantEnd: 20}, // bottom clamped
		{total: 0, cursor: 0, size: 8, wantStart: 0, wantEnd: 0},     // empty
	}
	for _, c := range cases {
		gs, ge := scrollWindow(c.total, c.cursor, c.size)
		if gs != c.wantStart || ge != c.wantEnd {
			t.Errorf("scrollWindow(total=%d cursor=%d size=%d) = (%d,%d), want (%d,%d)",
				c.total, c.cursor, c.size, gs, ge, c.wantStart, c.wantEnd)
		}
	}
}

func TestRenderFailuresPanel(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 18, 0, 0, 0, time.UTC)

	empty := stripANSI(renderFailuresPanel(nil, now, 80, false, 0))
	if !strings.Contains(empty, "no failures in the last hour") {
		t.Errorf("missing empty message:\n%s", empty)
	}

	runs := []model.DagRun{
		{DagID: "etl", RunID: "run-42", State: model.DagRunFailed, Start: now.Add(-6 * time.Minute), End: now.Add(-1 * time.Minute)},
	}
	out := stripANSI(renderFailuresPanel(runs, now, 100, false, 0))
	for _, want := range []string{"Recent Failures (1)", "etl", "run-42", "1m ago", "5m"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
}

func TestRenderPoolsPanel_Bar(t *testing.T) {
	t.Parallel()

	empty := stripANSI(renderPoolsPanel(nil, 80))
	if !strings.Contains(empty, "no pools reported") {
		t.Errorf("missing empty message:\n%s", empty)
	}

	pools := []model.Pool{
		{Name: "default", Slots: 100, OccupiedSlots: 20},
		{Name: "priority", Slots: 10, OccupiedSlots: 9},
	}
	out := stripANSI(renderPoolsPanel(pools, 100))
	for _, want := range []string{"Pools (2)", "default", "20/100", " 20%", "priority", "9/10", " 90%"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
	// The bar itself uses █ / ░ glyphs.
	if !strings.Contains(out, "█") || !strings.Contains(out, "░") {
		t.Errorf("expected pool bar glyphs in output:\n%s", out)
	}
}

func TestPoolBar_UtilisationBounds(t *testing.T) {
	t.Parallel()

	// bar with u=0 should be all empty; u=1 should be all full.
	empty := stripANSI(renderPoolBar(0))
	if strings.Contains(empty, "█") {
		t.Errorf("u=0 must be all empty, got %q", empty)
	}
	full := stripANSI(renderPoolBar(1.5)) // clamped to 1
	if strings.Contains(full, "░") {
		t.Errorf("u=1 must be all full, got %q", full)
	}
	// Non-zero small u must show at least one filled cell.
	tiny := stripANSI(renderPoolBar(0.01))
	if !strings.Contains(tiny, "█") {
		t.Errorf("u=0.01 must render at least one █ cell, got %q", tiny)
	}
}

func TestView_ImportErrorsBadge(t *testing.T) {
	t.Parallel()

	s := store.New()
	s.UpsertImportErrors([]model.ImportError{
		{ID: 1, Filename: "dags/bad.py", Timestamp: time.Now()},
	})
	m := New(context.Background(), s, nil, "https://airflow.example.com", "0.0.0")
	m.width, m.height = 120, 30
	m.now = time.Now()

	out := stripANSI(m.View())
	if !strings.Contains(out, "1 import errors") {
		t.Errorf("badge missing:\n%s", out)
	}
}

func TestView_WideLayoutHasBothPanels(t *testing.T) {
	t.Parallel()

	// m.now is passed to RecentFailedDagRunsSince, but the seeded run's End
	// timestamp anchors to it too — use real time.Now() so the two match.
	now := time.Now()
	s := store.New()
	s.UpsertDagRuns([]model.DagRun{
		{DagID: "active", RunID: "r-a", State: model.DagRunRunning, Start: now.Add(-1 * time.Minute), UpdatedAt: now},
		{DagID: "flopped", RunID: "r-f", State: model.DagRunFailed, Start: now.Add(-5 * time.Minute), End: now.Add(-30 * time.Second), UpdatedAt: now},
	})
	m := New(context.Background(), s, nil, "https://x", "0")
	m.width, m.height = 140, 40
	m.now = now

	out := stripANSI(m.View())
	if !strings.Contains(out, "active") || !strings.Contains(out, "flopped") {
		t.Errorf("wide layout should include both panels:\n%s", out)
	}
}
