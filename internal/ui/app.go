// Package ui hosts the Bubble Tea model and panels that make up the afmetric
// TUI dashboard.
package ui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/store"
)

// tickRate controls how often the UI re-renders from the store.
const tickRate = 500 * time.Millisecond

// wideLayoutMinWidth is the min terminal width at which Active Runs and
// Failures render side-by-side.
const wideLayoutMinWidth = 100

const failureWindow = time.Hour

type tickMsg time.Time

type focusZone int

const (
	focusActive focusZone = iota
	focusFailures
)

type screen int

const (
	screenDashboard screen = iota
	screenTaskInstances
	screenTaskLogs
)

// Model is the top-level Bubble Tea model.
type Model struct {
	store   *store.Store
	fetcher Fetcher
	ctx     context.Context //nolint:containedctx // Bubble Tea Cmd closures need it and it's a UI-scoped derived context.
	baseURL string
	version string

	now    time.Time
	width  int
	height int

	// Dashboard selection.
	focus  focusZone
	cursor int

	// Drill-down state (task instances).
	screen       screen
	drillDagID   string
	drillRunID   string
	drillLoading bool
	drillItems   []model.TaskInstance
	drillErr     string
	drillCursor  int // selected task instance row on the drill screen

	// Log viewer state (screenTaskLogs).
	logDagID, logRunID, logTaskID string
	logTryNumber                  int             // try_number of the log currently loaded
	logCurrentTry                 int             // TI.TryNumber at drill-entry — the *live* attempt, may still be growing
	logCurrentState               model.TaskState // TI.State at drill-entry — used to seed attempts[current].State
	logAttempts                   []model.TaskAttempt
	logTryIdx                     int // index into logAttempts of the current try
	logLoading                    bool
	logErr                        string
	logContent                    string
	logOffset                     int // top line currently shown
	logHeight                     int // viewport height, cached from window size
	// Per-try log cache — keyed by try_number, populated on successful fetch
	// of a *terminal* attempt (past retries) and wiped on drill-entry so
	// switching tasks always sees fresh content. The live attempt
	// (m.logCurrentTry) is intentionally never cached — its log grows over
	// time and stale snapshots would silently hide new output. `[` / `]`
	// toggle between already-loaded past attempts without another API round
	// trip; the live attempt always refetches.
	logCache map[int]string

	// Log search state.
	logSearchMode    bool   // true while user is typing a pattern (footer input)
	logSearchInput   string // pattern being typed
	logSearchActive  string // last committed pattern (empty = no highlight)
	logSearchMatches []int  // line indices with a match, ascending
	logSearchIdx     int    // index into logSearchMatches for current match
}

// New builds a Model bound to a Store and a Fetcher.
func New(ctx context.Context, s *store.Store, f Fetcher, baseURL, version string) Model {
	return Model{
		store:   s,
		fetcher: f,
		ctx:     ctx,
		baseURL: baseURL,
		version: version,
		now:     time.Now(),
		width:   80,
		height:  24,
	}
}

// Init satisfies tea.Model.
func (m Model) Init() tea.Cmd { return tick() }

func tick() tea.Cmd {
	return tea.Tick(tickRate, func(t time.Time) tea.Msg { return tickMsg(t) })
}

// Update satisfies tea.Model.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		// Log viewport = full body minus header/footer/panel chrome (~6 lines).
		m.logHeight = msg.Height - 6
		if m.logHeight < 5 {
			m.logHeight = 5
		}
		return m, nil

	case tickMsg:
		m.now = time.Time(msg)
		return m, tick()

	case taskInstancesLoadedMsg:
		if msg.dagID == m.drillDagID && msg.runID == m.drillRunID {
			m.drillLoading = false
			m.drillItems = msg.items
			m.drillErr = ""
			// Clamp cursor to new list length.
			if m.drillCursor >= len(m.drillItems) {
				m.drillCursor = 0
			}
		}
		return m, nil

	case taskInstancesErrMsg:
		if msg.dagID == m.drillDagID && msg.runID == m.drillRunID {
			m.drillLoading = false
			m.drillErr = msg.err.Error()
		}
		return m, nil

	case taskLogsLoadedMsg:
		// Only apply if this reply matches the *currently-viewed* try in the
		// log viewer. Cross-drill leaks, out-of-order refetches for the same
		// try, and post-esc responses are all filtered out here.
		if m.screen == screenTaskLogs &&
			msg.dagID == m.logDagID && msg.runID == m.logRunID && msg.taskID == m.logTaskID &&
			msg.tryNumber == m.logTryNumber {
			parsed := parseAirflowLogContent(msg.content)
			// Never cache the live attempt — its log grows and a stale
			// snapshot would silently hide new output on the next `]` back.
			// Past attempts are terminal, cache away.
			if msg.tryNumber != m.logCurrentTry {
				if m.logCache == nil {
					m.logCache = make(map[int]string)
				}
				m.logCache[msg.tryNumber] = parsed
			}
			m.logLoading = false
			m.logContent = parsed
			m.logErr = ""
			m.logOffset = 0
			m.recomputeLogMatches()
		}
		return m, nil

	case taskLogsErrMsg:
		if m.screen == screenTaskLogs &&
			msg.dagID == m.logDagID && msg.runID == m.logRunID && msg.taskID == m.logTaskID &&
			msg.tryNumber == m.logTryNumber {
			m.logLoading = false
			m.logErr = msg.err.Error()
		}
		return m, nil

	case taskTriesLoadedMsg:
		if m.screen == screenTaskLogs &&
			msg.dagID == m.logDagID && msg.runID == m.logRunID && msg.taskID == m.logTaskID {
			m.logAttempts = buildAttempts(m.logCurrentTry, m.logCurrentState, msg.attempts)
			m.logTryIdx = indexOfTry(m.logAttempts, m.logTryNumber)
		}
		return m, nil

	case taskTriesErrMsg:
		// /tries can be empty or unavailable depending on Airflow version and
		// the task's history. That's fine — the log endpoint still serves each
		// try by path, so we synthesise [1..currentTry] and let the user page.
		if m.screen == screenTaskLogs &&
			msg.dagID == m.logDagID && msg.runID == m.logRunID && msg.taskID == m.logTaskID {
			m.logAttempts = buildAttempts(m.logCurrentTry, m.logCurrentState, nil)
			m.logTryIdx = indexOfTry(m.logAttempts, m.logTryNumber)
		}
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Accept Q/q on both US and Russian JCUKEN layouts so quit works
	// regardless of what the OS is sending.
	switch msg.String() {
	case "q", "Q", "й", "Й", "ctrl+c":
		return m, tea.Quit
	}

	if m.screen == screenTaskInstances {
		switch msg.String() {
		case "esc", "backspace", "left", "h":
			m.screen = screenDashboard
			return m, nil
		case "r", "R", "к", "К":
			if m.fetcher == nil || m.drillDagID == "" {
				return m, nil
			}
			m.drillLoading = true
			m.drillErr = ""
			return m, fetchTaskInstancesCmd(m.ctx, m.fetcher, m.drillDagID, m.drillRunID)
		case "up", "k":
			if m.drillCursor > 0 {
				m.drillCursor--
			}
			return m, nil
		case "down", "j":
			if m.drillCursor < len(m.drillItems)-1 {
				m.drillCursor++
			}
			return m, nil
		case "enter":
			ti := m.selectedTaskInstance()
			if ti == nil || m.fetcher == nil {
				return m, nil
			}
			m.screen = screenTaskLogs
			m.logDagID = ti.DagID
			m.logRunID = ti.RunID
			m.logTaskID = ti.TaskID
			m.logTryNumber = ti.TryNumber
			m.logCurrentTry = ti.TryNumber
			m.logCurrentState = ti.State
			// Attempts stays nil until /tries answers. During the loading
			// window renderTryCounter shows `(try N)` (total==0 branch) and
			// switchTryIdx short-circuits on len<=1, so no page-through until
			// we know the real range.
			m.logAttempts = nil
			m.logTryIdx = 0
			m.logContent = ""
			m.logErr = ""
			m.logOffset = 0
			// Fresh drill — drop any per-try log cache from the previous task.
			m.logCache = make(map[int]string)
			m.clearLogSearch()
			// Queued / never-run tasks report TryNumber == 0 and the /logs/0
			// endpoint 404s. Skip the fetch and show a placeholder instead of
			// pretending to load.
			if ti.TryNumber < 1 {
				m.logLoading = false
				m.logContent = "(task has not run yet)"
				return m, nil
			}
			m.logLoading = true
			return m, tea.Batch(
				fetchTaskLogsCmd(m.ctx, m.fetcher, ti.DagID, ti.RunID, ti.TaskID, ti.TryNumber),
				fetchTaskTriesCmd(m.ctx, m.fetcher, ti.DagID, ti.RunID, ti.TaskID),
			)
		}
		return m, nil
	}

	if m.screen == screenTaskLogs {
		// Search input mode short-circuits everything except esc/enter/backspace.
		if m.logSearchMode {
			return m.handleLogSearchInput(msg)
		}
		switch msg.String() {
		case "esc":
			// If there's an active highlight, first press clears it; second
			// press pops back to task instances.
			if m.logSearchActive != "" {
				m.clearLogSearch()
				return m, nil
			}
			m.screen = screenTaskInstances
			return m, nil
		case "backspace", "left", "h":
			m.screen = screenTaskInstances
			return m, nil
		case "r", "R", "к", "К":
			if m.fetcher == nil {
				return m, nil
			}
			// Evict the current try from cache so the refetch actually hits
			// the API — running tasks can grow their log between navigations.
			delete(m.logCache, m.logTryNumber)
			m.logLoading = true
			m.logErr = ""
			return m, fetchTaskLogsCmd(m.ctx, m.fetcher, m.logDagID, m.logRunID, m.logTaskID, m.logTryNumber)
		case "up", "k":
			if m.logOffset > 0 {
				m.logOffset--
			}
			return m, nil
		case "down", "j":
			m.logOffset = clampLogOffset(m.logOffset+1, m.logContent, m.logHeight)
			return m, nil
		case "g", "home":
			m.logOffset = 0
			return m, nil
		case "G", "end":
			m.logOffset = clampLogOffset(1<<30, m.logContent, m.logHeight)
			return m, nil
		case "[":
			return m.switchTryIdx(m.logTryIdx - 1)
		case "]":
			return m.switchTryIdx(m.logTryIdx + 1)
		case "/":
			m.logSearchMode = true
			m.logSearchInput = ""
			return m, nil
		case "n":
			return m.jumpToMatch(+1), nil
		case "N":
			return m.jumpToMatch(-1), nil
		}
		return m, nil
	}

	// Dashboard bindings.
	switch msg.String() {
	case "tab":
		if m.focus == focusActive {
			m.focus = focusFailures
		} else {
			m.focus = focusActive
		}
		m.cursor = 0
		return m, nil
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
		return m, nil
	case "down", "j":
		if m.cursor < m.focusedListLen()-1 {
			m.cursor++
		}
		return m, nil
	case "enter":
		run := m.selectedRun()
		if run == nil || m.fetcher == nil {
			return m, nil
		}
		m.screen = screenTaskInstances
		m.drillDagID = run.DagID
		m.drillRunID = run.RunID
		m.drillLoading = true
		m.drillErr = ""
		m.drillItems = nil
		return m, fetchTaskInstancesCmd(m.ctx, m.fetcher, run.DagID, run.RunID)
	}
	return m, nil
}

func (m Model) focusedList() []model.DagRun {
	if m.focus == focusActive {
		return m.store.ActiveDagRuns()
	}
	return m.store.RecentFailedDagRunsSince(m.now, failureWindow)
}

func (m Model) focusedListLen() int { return len(m.focusedList()) }

func (m Model) selectedTaskInstance() *model.TaskInstance {
	if m.drillCursor < 0 || m.drillCursor >= len(m.drillItems) {
		return nil
	}
	ti := m.drillItems[m.drillCursor]
	return &ti
}

// clampLogOffset keeps the log viewport offset within [0, maxLines-height].
func clampLogOffset(off int, content string, height int) int {
	if off < 0 {
		return 0
	}
	if height <= 0 {
		return 0
	}
	total := 1
	for _, r := range content {
		if r == '\n' {
			total++
		}
	}
	maxOff := total - height
	if maxOff < 0 {
		maxOff = 0
	}
	if off > maxOff {
		return maxOff
	}
	return off
}

func (m Model) selectedRun() *model.DagRun {
	list := m.focusedList()
	if len(list) == 0 || m.cursor < 0 || m.cursor >= len(list) {
		return nil
	}
	run := list[m.cursor]
	return &run
}

// View satisfies tea.Model.
func (m Model) View() string {
	switch m.screen {
	case screenTaskInstances:
		return m.viewTaskInstances()
	case screenTaskLogs:
		return m.viewTaskLogs()
	default:
		return m.viewDashboard()
	}
}

func (m Model) viewDashboard() string {
	fullW := m.contentWidth()

	header := m.renderHeader()
	footer := m.renderDashboardFooter()
	health := renderHealthPanel(m.store.Health(), m.now, fullW)
	pools := renderPoolsPanel(m.store.Pools(), fullW)

	active := m.store.ActiveDagRuns()
	failures := m.store.RecentFailedDagRunsSince(m.now, failureWindow)

	// Clamp cursor to the currently focused list.
	cursor := m.cursor
	if cursor >= m.focusedListLen() {
		cursor = m.focusedListLen() - 1
		if cursor < 0 {
			cursor = 0
		}
	}

	waitLookup := m.store.WaitingCount

	var middle string
	if fullW >= wideLayoutMinWidth {
		half := (fullW - 2) / 2
		left := renderActiveRunsPanel(active, waitLookup, m.now, half, m.focus == focusActive, cursor)
		right := renderFailuresPanel(failures, m.now, half, m.focus == focusFailures, cursor)
		// Pad the shorter side up to the taller side's height so the panel
		// borders line up regardless of how many rows each side has.
		left, right = matchHeights(left, right)
		middle = lipgloss.JoinHorizontal(lipgloss.Top, left, "  ", right)
	} else {
		middle = lipgloss.JoinVertical(lipgloss.Left,
			renderActiveRunsPanel(active, waitLookup, m.now, fullW, m.focus == focusActive, cursor),
			renderFailuresPanel(failures, m.now, fullW, m.focus == focusFailures, cursor),
		)
	}

	return lipgloss.JoinVertical(lipgloss.Left, header, health, middle, pools, footer)
}

func (m Model) viewTaskInstances() string {
	header := m.renderHeader()
	footer := styleMuted.Render("esc back · ↑↓/jk move · enter logs · r refresh · q quit")

	panel := renderTaskInstancesPanel(taskInstancesViewState{
		dagID:   m.drillDagID,
		runID:   m.drillRunID,
		loading: m.drillLoading,
		errMsg:  m.drillErr,
		items:   m.drillItems,
		width:   m.contentWidth(),
		cursor:  m.drillCursor,
	})
	return lipgloss.JoinVertical(lipgloss.Left, header, panel, footer)
}

func (m Model) viewTaskLogs() string {
	header := m.renderHeader()

	multipleAttempts := len(m.logAttempts) > 1

	var footer string
	switch {
	case m.logSearchMode:
		footer = styleTitle.Render("/") + m.logSearchInput + styleMuted.Render("_    (enter=find, esc=cancel)")
	case m.logSearchActive != "":
		footer = styleMuted.Render("esc clear · n/N next/prev match · ↑↓/jk scroll · [/] try · r refresh · q quit")
	default:
		hints := "esc back · ↑↓/jk scroll · g/G top/bot · / search · r refresh · q quit"
		if multipleAttempts {
			hints = "esc back · ↑↓/jk scroll · [/] prev/next try · / search · g/G top/bot · r refresh · q quit"
		}
		footer = styleMuted.Render(hints)
	}

	panel := renderLogPanel(logPanelState{
		dagID:        m.logDagID,
		runID:        m.logRunID,
		taskID:       m.logTaskID,
		tryNumber:    m.logTryNumber,
		attemptIdx:   m.logTryIdx,
		attemptTotal: len(m.logAttempts),
		loading:      m.logLoading,
		errMsg:       m.logErr,
		content:      m.logContent,
		offset:       m.logOffset,
		height:       m.logHeight,
		width:        m.contentWidth(),
		searchTerm:   m.logSearchActive,
		matchLines:   m.logSearchMatches,
		matchIdx:     m.logSearchIdx,
	})
	return lipgloss.JoinVertical(lipgloss.Left, header, panel, footer)
}

func (m Model) renderHeader() string {
	left := styleHeader.Render("afmetric ") + styleMuted.Render(m.version)
	right := styleMuted.Render(m.baseURL)

	errCount := len(m.store.ImportErrors())
	badge := ""
	if errCount > 0 {
		badge = "  " + styleUnhealthy.Render(fmt.Sprintf("⚠ %d import errors", errCount))
	}

	leftPart := left + badge
	pad := m.width - lipgloss.Width(leftPart) - lipgloss.Width(right)
	if pad < 1 {
		pad = 1
	}
	return leftPart + strings.Repeat(" ", pad) + right
}

func (m Model) renderDashboardFooter() string {
	return styleMuted.Render("tab focus · ↑↓/jk move · enter drill · q quit")
}

func (m Model) contentWidth() int {
	w := m.width - 2
	if w < 40 {
		return 40
	}
	return w
}

// switchTryIdx moves to another attempt by its position in logAttempts.
// No-op on out-of-range or when the list contains a single item.
//
// Past attempts served from logCache are instant (no fetch, no loading
// spinner). The live attempt (== m.logCurrentTry) is always refetched —
// its log grows over time, so a cached snapshot would silently hide new
// output. See the logCache field comment for the invariant.
func (m Model) switchTryIdx(target int) (tea.Model, tea.Cmd) {
	if m.fetcher == nil || len(m.logAttempts) <= 1 {
		return m, nil
	}
	if target < 0 || target >= len(m.logAttempts) || target == m.logTryIdx {
		return m, nil
	}
	m.logTryIdx = target
	m.logTryNumber = m.logAttempts[target].TryNumber
	m.logErr = ""
	m.logOffset = 0
	m.clearLogSearch()
	if m.logTryNumber != m.logCurrentTry {
		if cached, ok := m.logCache[m.logTryNumber]; ok {
			m.logLoading = false
			m.logContent = cached
			m.recomputeLogMatches()
			return m, nil
		}
	}
	m.logLoading = true
	m.logContent = ""
	return m, fetchTaskLogsCmd(m.ctx, m.fetcher, m.logDagID, m.logRunID, m.logTaskID, m.logTryNumber)
}

// indexOfTry returns the position of tryNumber in attempts, or 0 if not
// found (defensive — normally the current try_number is present).
func indexOfTry(attempts []model.TaskAttempt, tryNumber int) int {
	for i, a := range attempts {
		if a.TryNumber == tryNumber {
			return i
		}
	}
	return 0
}

// buildAttempts constructs the navigable attempt list for the log viewer.
//
// The log endpoint (/logs/{task_try_number}) serves *every* attempt by path
// regardless of whether the /tries history table has a row for it, so we
// synthesise the full [1..N] range and let /tries fill in state/timing.
//
// N is max(currentTry, max hist.TryNumber). Taking the maximum guards
// against a retry that landed on the server between the TaskInstance fetch
// and the /tries fetch — the row for the new attempt would otherwise be
// silently dropped by a strict currentTry clamp.
//
// currentState is the live TI state at drill-entry — it wins over any
// hist row for the current try, because TaskInstanceHistory snapshots
// *pre-transition* state and TI is fresher.
//
// Returns nil for currentTry < 1 (queued / never-run task): no attempts to
// paginate through, and switchTryIdx's len<=1 short-circuit keeps [ / ]
// as a no-op.
func buildAttempts(currentTry int, currentState model.TaskState, hist []model.TaskAttempt) []model.TaskAttempt {
	effective := currentTry
	for _, a := range hist {
		if a.TryNumber > effective {
			effective = a.TryNumber
		}
	}
	if effective < 1 {
		return nil
	}
	byNum := make(map[int]model.TaskAttempt, effective)
	for i := 1; i <= effective; i++ {
		byNum[i] = model.TaskAttempt{TryNumber: i}
	}
	for _, a := range hist {
		if a.TryNumber >= 1 {
			byNum[a.TryNumber] = a
		}
	}
	if currentTry >= 1 {
		byNum[currentTry] = model.TaskAttempt{TryNumber: currentTry, State: currentState}
	}
	out := make([]model.TaskAttempt, 0, effective)
	for i := 1; i <= effective; i++ {
		out = append(out, byNum[i])
	}
	return out
}

// clearLogSearch drops the active pattern and its match list. Called on esc
// with an active highlight, on try switch, on log reload.
func (m *Model) clearLogSearch() {
	m.logSearchMode = false
	m.logSearchInput = ""
	m.logSearchActive = ""
	m.logSearchMatches = nil
	m.logSearchIdx = 0
}

// handleLogSearchInput consumes keystrokes while the pattern is being typed
// in the footer. Enter commits, Esc aborts, Backspace edits, printable
// runes append.
func (m Model) handleLogSearchInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		m.logSearchMode = false
		m.logSearchInput = ""
		return m, nil
	case tea.KeyEnter:
		m.logSearchMode = false
		m.logSearchActive = m.logSearchInput
		m.recomputeLogMatches()
		if len(m.logSearchMatches) > 0 {
			m.logSearchIdx = 0
			m.logOffset = clampLogOffset(m.logSearchMatches[0], m.logContent, m.logHeight)
		}
		return m, nil
	case tea.KeyBackspace:
		if n := len(m.logSearchInput); n > 0 {
			m.logSearchInput = m.logSearchInput[:n-1]
		}
		return m, nil
	case tea.KeyRunes, tea.KeySpace:
		m.logSearchInput += string(msg.Runes)
		return m, nil
	}
	return m, nil
}

// recomputeLogMatches finds every content line containing logSearchActive
// (case-insensitive) and stashes their 0-based line indices in ascending
// order. Idempotent.
func (m *Model) recomputeLogMatches() {
	m.logSearchMatches = nil
	m.logSearchIdx = 0
	if m.logSearchActive == "" || m.logContent == "" {
		return
	}
	needle := strings.ToLower(m.logSearchActive)
	for i, line := range strings.Split(m.logContent, "\n") {
		if strings.Contains(strings.ToLower(line), needle) {
			m.logSearchMatches = append(m.logSearchMatches, i)
		}
	}
}

// jumpToMatch advances the current match index by dir (+1 next, -1 prev)
// with wrap-around, and scrolls the viewport so the match is visible.
func (m Model) jumpToMatch(dir int) Model {
	if len(m.logSearchMatches) == 0 {
		return m
	}
	n := len(m.logSearchMatches)
	m.logSearchIdx = (m.logSearchIdx + dir + n) % n
	m.logOffset = clampLogOffset(m.logSearchMatches[m.logSearchIdx], m.logContent, m.logHeight)
	return m
}

// matchHeights pads whichever of a/b has fewer lines with blank lines of the
// same visible width, so JoinHorizontal produces two panels of equal height
// and their bottom borders align.
func matchHeights(a, b string) (string, string) {
	ah := lipgloss.Height(a)
	bh := lipgloss.Height(b)
	switch {
	case ah < bh:
		a += strings.Repeat("\n"+strings.Repeat(" ", lipgloss.Width(a)), bh-ah)
	case bh < ah:
		b += strings.Repeat("\n"+strings.Repeat(" ", lipgloss.Width(b)), ah-bh)
	}
	return a, b
}
