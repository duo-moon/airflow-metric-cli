package ui

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// airflowLevelPattern matches an Airflow log line's level token:
//
//	[TS] {file.py:LN} INFO - msg
//
// Requires whitespace on both sides and the " - " suffix so we don't paint
// the word "INFO" appearing inside a message body.
var airflowLevelPattern = regexp.MustCompile(`(\s)(DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|FATAL)(\s+-\s)`)

// airflowLogTuplePattern extracts the log body from Airflow's
// `[('host', 'log text')]` Python-repr wrapper. Anchored to the comma so we
// only capture the second element of each tuple (the body), not the host.
var airflowLogTuplePattern = regexp.MustCompile(`(?s),\s*['"](.*?)['"]\s*\)`)

// parseAirflowLogContent unpeels the Python-repr wrapper Airflow returns for
// task logs, then converts literal \n / \t escapes into real characters so
// the log renders line-by-line.
func parseAirflowLogContent(s string) string {
	if strings.HasPrefix(s, "[(") && strings.HasSuffix(s, ")]") {
		matches := airflowLogTuplePattern.FindAllStringSubmatch(s, -1)
		if len(matches) > 0 {
			parts := make([]string, 0, len(matches))
			for _, m := range matches {
				parts = append(parts, m[1])
			}
			s = strings.Join(parts, "\n")
		}
	}
	s = strings.ReplaceAll(s, `\n`, "\n")
	s = strings.ReplaceAll(s, `\t`, "\t")
	return s
}

// logLevel returns the detected severity token for an Airflow log line, or
// "" if none matched. Kept separate from styling so it can be tested without
// a real TTY.
func logLevel(line string) string {
	m := airflowLevelPattern.FindStringSubmatch(line)
	if m == nil {
		return ""
	}
	return m[2]
}

// colorizeLogLine paints only the level token itself (INFO / WARNING / …)
// so the surrounding timestamp and message stay in the default colour.
// Unrecognised lines pass through unchanged.
func colorizeLogLine(line string) string {
	return airflowLevelPattern.ReplaceAllStringFunc(line, func(match string) string {
		sub := airflowLevelPattern.FindStringSubmatch(match)
		lead, level, tail := sub[1], sub[2], sub[3]
		var painted string
		switch level {
		case "ERROR", "CRITICAL", "FATAL":
			painted = styleUnhealthy.Render(level)
		case "WARNING", "WARN":
			painted = styleUnknown.Render(level)
		case "INFO":
			painted = styleHealthy.Render(level)
		case "DEBUG":
			painted = styleMuted.Render(level)
		default:
			return match
		}
		return lead + painted + tail
	})
}

// styleSearchHit is used to invert-highlight substring matches inside a log
// line — chosen to survive on top of the level-token colours.
var styleSearchHit = lipgloss.NewStyle().
	Background(lipgloss.Color("226")). // bright yellow
	Foreground(lipgloss.Color("16")).  // near-black
	Bold(true)

// highlightMatches wraps every case-insensitive occurrence of needle in
// line with styleSearchHit. Called *after* colorizeLogLine — since it
// operates on the raw text before ANSI codes were injected, we call it
// first and let colorization run on the highlighted string.
func highlightMatches(line, needle string) string {
	if needle == "" {
		return line
	}
	lowerLine := strings.ToLower(line)
	lowerNeedle := strings.ToLower(needle)
	var b strings.Builder
	i := 0
	for {
		j := strings.Index(lowerLine[i:], lowerNeedle)
		if j < 0 {
			b.WriteString(line[i:])
			return b.String()
		}
		start := i + j
		end := start + len(needle)
		b.WriteString(line[i:start])
		b.WriteString(styleSearchHit.Render(line[start:end]))
		i = end
	}
}

type logPanelState struct {
	dagID        string
	runID        string
	taskID       string
	tryNumber    int
	attemptIdx   int // 0-based position of tryNumber inside the /tries list
	attemptTotal int // total attempts recorded on the server (incl. clears)
	loading      bool
	errMsg       string
	content      string
	offset       int
	height       int
	width        int

	// Search state.
	searchTerm string // "" when highlight is inactive
	matchLines []int  // line indices with a match, ascending
	matchIdx   int    // index of the current match inside matchLines
}

func renderLogPanel(s logPanelState) string {
	title := styleTitle.Render(fmt.Sprintf("Logs — %s / %s / %s ",
		s.dagID, truncateStr(s.runID, 30), s.taskID)) + renderTryCounter(s.attemptIdx, s.attemptTotal, s.tryNumber)

	if s.loading {
		body := styleMuted.Render("loading logs…")
		return stylePanel.Width(s.width).Render(title + "\n" + body)
	}
	if s.errMsg != "" {
		body := styleError.Render("error: " + s.errMsg)
		return stylePanel.Width(s.width).Render(title + "\n" + body)
	}
	if s.content == "" {
		body := styleMuted.Render("(empty log)")
		return stylePanel.Width(s.width).Render(title + "\n" + body)
	}

	lines := strings.Split(s.content, "\n")
	h := s.height
	if h <= 0 {
		h = 20
	}

	off := s.offset
	if off < 0 {
		off = 0
	}
	if off > len(lines)-1 {
		off = len(lines) - 1
	}
	end := off + h
	if end > len(lines) {
		end = len(lines)
	}

	visible := make([]string, end-off)
	for i, l := range lines[off:end] {
		if s.searchTerm != "" {
			l = highlightMatches(l, s.searchTerm)
		}
		visible[i] = colorizeLogLine(l)
	}

	// Position line: base "lines A–B of N", optionally with match counter.
	posLine := styleMuted.Render(fmt.Sprintf("lines %d–%d of %d", off+1, end, len(lines)))
	if s.searchTerm != "" {
		if len(s.matchLines) == 0 {
			posLine += "  ·  " + styleMuted.Render(fmt.Sprintf("no matches for %q", s.searchTerm))
		} else {
			posLine += "  ·  " + styleTitle.Render(fmt.Sprintf("match %d/%d %q",
				s.matchIdx+1, len(s.matchLines), s.searchTerm))
		}
	}

	return stylePanel.Width(s.width).Render(
		title + "\n" +
			posLine + "\n" +
			strings.Join(visible, "\n"),
	)
}

// renderTryCounter renders "(try N/M)" using the position of the currently
// viewed attempt inside the /tries list (N = idx+1) and the total number of
// recorded attempts (M). idx and total are derived from the /tries endpoint,
// so they correctly account for manual clears as well as retries — unlike
// TaskInstance.MaxTries which only counts retries.
//
// Colour:
//   - muted when M == 1 (nothing to page through)
//   - yellow when M > 1 and the latest attempt is on screen
//   - yellow bold when the user is looking at an older attempt
//
// tryNumber is only used as a fallback label when total == 0 (i.e. /tries
// hasn't answered yet or errored) — we then render "(try N)" without a
// total.
func renderTryCounter(idx, total, tryNumber int) string {
	if total <= 0 {
		return styleMuted.Render(fmt.Sprintf("(try %d)", tryNumber))
	}
	current := idx + 1
	if current < 1 {
		current = 1
	}
	if current > total {
		current = total
	}
	text := fmt.Sprintf("(try %d/%d)", current, total)
	switch {
	case total == 1:
		return styleMuted.Render(text)
	case current < total:
		return styleUnknown.Bold(true).Render(text)
	default:
		return styleUnknown.Render(text)
	}
}
