package ui

import (
	"strings"
	"testing"
)

func TestLogLevel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		line string
		want string
	}{
		{"info", "[2026-08-17T20:50:00+00:00] {taskinstance.py:1} INFO - starting", "INFO"},
		{"warning", "[ts] {file.py:1} WARNING - deprecated", "WARNING"},
		{"warn short", "[ts] {file.py:1} WARN - short form", "WARN"},
		{"error", "[ts] {file.py:1} ERROR - boom", "ERROR"},
		{"critical", "[ts] {file.py:1} CRITICAL - kaboom", "CRITICAL"},
		{"debug", "[ts] {file.py:1} DEBUG - noise", "DEBUG"},
		{"continuation traceback", `  File "/x.py", line 42, in foo`, ""},
		{"info word inside message", "message mentions INFO but not as a level", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := logLevel(c.line); got != c.want {
				t.Errorf("logLevel(%q) = %q, want %q", c.line, got, c.want)
			}
		})
	}
}

func TestColorizeLogLine_LeavesRestUntouched(t *testing.T) {
	t.Parallel()

	in := "[2026-08-17T20:50:00+00:00] {taskinstance.py:1} INFO - job started"
	got := colorizeLogLine(in)

	// Stripped of ANSI, the visible text must be exactly the input.
	if stripped := stripANSI(got); stripped != in {
		t.Errorf("visible text changed: got %q, want %q", stripped, in)
	}
}

func TestParseAirflowLogContent(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "plain multiline stays untouched",
			in:   "line1\nline2\nline3",
			want: "line1\nline2\nline3",
		},
		{
			// Non-tuple content passes through untouched: literal `\n` in a
			// v3-decoded structlog payload (which may legitimately contain
			// escaped-in-JSON sequences alongside real newlines) must not be
			// re-expanded — that would double-break tracebacks. Escape
			// expansion is scoped to the Python-repr tuple branch.
			name: "non-tuple content preserves literal backslash-n",
			in:   `line1\nline2\nline3`,
			want: `line1\nline2\nline3`,
		},
		{
			name: "python-repr wrapper unwraps",
			in:   `[('305167424ec9', ' INFO - starting\n INFO - done\n')]`,
			want: " INFO - starting\n INFO - done\n",
		},
		{
			name: "double-quoted variant",
			in:   `[('host', " INFO - ok\n")]`,
			want: " INFO - ok\n",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := parseAirflowLogContent(c.in)
			if got != c.want {
				t.Errorf("got %q\nwant %q", got, c.want)
			}
			// Sanity: result must contain at least as many newlines as
			// the human-visible expectation.
			if strings.Count(got, "\n") != strings.Count(c.want, "\n") {
				t.Errorf("newline count differs: got %d, want %d", strings.Count(got, "\n"), strings.Count(c.want, "\n"))
			}
		})
	}
}
