package ui

import (
	"strings"
	"testing"
)

func TestRecomputeLogMatches_CaseInsensitive(t *testing.T) {
	t.Parallel()

	m := Model{
		logContent: strings.Join([]string{
			"INFO - fetching batch 1",
			"warning - approaching sla",
			"ERROR - upstream returned 500",
			"info - retry scheduled",
		}, "\n"),
		logSearchActive: "ERROR",
	}
	m.recomputeLogMatches()
	if got, want := len(m.logSearchMatches), 1; got != want {
		t.Fatalf("matches = %d, want %d", got, want)
	}
	if m.logSearchMatches[0] != 2 {
		t.Errorf("match line = %d, want 2", m.logSearchMatches[0])
	}

	m.logSearchActive = "info"
	m.recomputeLogMatches()
	if got, want := len(m.logSearchMatches), 2; got != want {
		t.Fatalf("case-insensitive info matches = %d, want %d", got, want)
	}
}

func TestJumpToMatch_WrapAround(t *testing.T) {
	t.Parallel()

	m := Model{
		logContent:       "a\nb\nc",
		logSearchActive:  "x",
		logSearchMatches: []int{0, 2}, // pretend they exist
		logSearchIdx:     0,
		logHeight:        10,
	}
	m = m.jumpToMatch(+1)
	if m.logSearchIdx != 1 {
		t.Errorf("after next, idx = %d, want 1", m.logSearchIdx)
	}
	m = m.jumpToMatch(+1) // wraps to 0
	if m.logSearchIdx != 0 {
		t.Errorf("after wrap-next, idx = %d, want 0", m.logSearchIdx)
	}
	m = m.jumpToMatch(-1) // wraps to end
	if m.logSearchIdx != 1 {
		t.Errorf("after wrap-prev, idx = %d, want 1", m.logSearchIdx)
	}
}

func TestClearLogSearch(t *testing.T) {
	t.Parallel()

	m := &Model{
		logSearchMode:    true,
		logSearchInput:   "err",
		logSearchActive:  "err",
		logSearchMatches: []int{1, 2},
		logSearchIdx:     1,
	}
	m.clearLogSearch()
	if m.logSearchMode || m.logSearchInput != "" || m.logSearchActive != "" ||
		len(m.logSearchMatches) != 0 || m.logSearchIdx != 0 {
		t.Errorf("clearLogSearch left residue: %+v", m)
	}
}

func TestRenderTryCounter_ChoosesStyle(t *testing.T) {
	t.Parallel()

	// We can't assert ANSI here (no TTY), but we can at least assert the
	// visible text and that the function doesn't panic on boundary values.
	cases := []struct {
		idx, total, tryNo int
		want              string
	}{
		{0, 1, 1, "(try 1/1)"}, // single attempt
		{4, 5, 5, "(try 5/5)"}, // latest of five
		{2, 5, 3, "(try 3/5)"}, // older attempt
		{0, 0, 7, "(try 7)"},   // /tries not answered yet — fall back to raw number
		{9, 3, 1, "(try 3/3)"}, // idx overshoot — clamped
	}
	for _, c := range cases {
		got := stripANSI(renderTryCounter(c.idx, c.total, c.tryNo))
		if got != c.want {
			t.Errorf("renderTryCounter(idx=%d,total=%d,try=%d) = %q, want %q",
				c.idx, c.total, c.tryNo, got, c.want)
		}
	}
}

func TestHighlightMatches_InvertsOnlyHits(t *testing.T) {
	t.Parallel()

	// Visible text (after stripping ANSI) must be unchanged.
	in := "ERROR upstream ERROR downstream"
	got := stripANSI(highlightMatches(in, "error"))
	if got != in {
		t.Errorf("stripped highlight = %q, want %q", got, in)
	}

	// Without a needle: no-op.
	if highlightMatches("hello", "") != "hello" {
		t.Errorf("empty needle must be a no-op")
	}
}
