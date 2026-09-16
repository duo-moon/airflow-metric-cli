package ui

import (
	"context"
	"testing"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/store"
)

func TestBuildAttempts_SynthesisesFullRange(t *testing.T) {
	t.Parallel()

	// /tries came back empty. We still expect [1..6] so [/] navigation works,
	// with the current try picking up its live state from currentState.
	got := buildAttempts(6, model.TaskFailed, nil)
	if len(got) != 6 {
		t.Fatalf("len = %d, want 6", len(got))
	}
	for i, a := range got {
		if a.TryNumber != i+1 {
			t.Errorf("attempt[%d].TryNumber = %d, want %d", i, a.TryNumber, i+1)
		}
	}
	if got[5].State != model.TaskFailed {
		t.Errorf("current attempt state = %q, want failed (from currentState)", got[5].State)
	}
}

func TestBuildAttempts_MergesHistoryStates(t *testing.T) {
	t.Parallel()

	// Task on try 4 (success). /tries returned rows for 1..3.
	hist := []model.TaskAttempt{
		{TryNumber: 1, State: model.TaskFailed},
		{TryNumber: 2, State: model.TaskFailed},
		{TryNumber: 3, State: model.TaskFailed},
	}
	got := buildAttempts(4, model.TaskSuccess, hist)
	if len(got) != 4 {
		t.Fatalf("len = %d, want 4", len(got))
	}
	for i := 0; i < 3; i++ {
		if got[i].State != model.TaskFailed {
			t.Errorf("attempt[%d].State = %q, want failed (from hist)", i, got[i].State)
		}
	}
	if got[3].State != model.TaskSuccess {
		t.Errorf("current attempt state = %q, want success (from currentState)", got[3].State)
	}
}

func TestBuildAttempts_CurrentStateOverridesHist(t *testing.T) {
	t.Parallel()

	// Airflow's /tries can echo the current try with pre-transition state —
	// TI's live state (currentState) is fresher.
	hist := []model.TaskAttempt{
		{TryNumber: 1, State: model.TaskFailed},
		{TryNumber: 2, State: model.TaskRunning},
	}
	got := buildAttempts(2, model.TaskSuccess, hist)
	if got[1].State != model.TaskSuccess {
		t.Errorf("current attempt state = %q, want success (currentState overrides hist)", got[1].State)
	}
}

func TestBuildAttempts_HistBeyondCurrentTryExtendsRange(t *testing.T) {
	t.Parallel()

	// Retry landed on the server between TI fetch (TryNumber=3) and /tries
	// fetch (rows 1..4). Silent-clamping to currentTry would lose try 4 —
	// take max instead.
	hist := []model.TaskAttempt{
		{TryNumber: 1, State: model.TaskFailed},
		{TryNumber: 2, State: model.TaskFailed},
		{TryNumber: 3, State: model.TaskFailed},
		{TryNumber: 4, State: model.TaskRunning},
	}
	got := buildAttempts(3, model.TaskFailed, hist)
	if len(got) != 4 {
		t.Fatalf("len = %d, want 4 (hist has a row beyond currentTry)", len(got))
	}
	if got[3].TryNumber != 4 || got[3].State != model.TaskRunning {
		t.Errorf("attempt[3] = %+v, want {TryNumber:4, State:running}", got[3])
	}
	// currentState still applies to the currentTry slot, not the extension.
	if got[2].State != model.TaskFailed {
		t.Errorf("attempt[2].State = %q, want failed (currentState for currentTry=3)", got[2].State)
	}
}

func TestBuildAttempts_ReturnsNilForQueuedTask(t *testing.T) {
	t.Parallel()

	// Queued / never-run task: TryNumber == 0. No attempts to paginate;
	// switchTryIdx will short-circuit on len<=1 and the counter renders
	// `(try 0)` via the total<=0 fallback.
	got := buildAttempts(0, model.TaskQueued, nil)
	if got != nil {
		t.Errorf("expected nil for queued task, got %+v", got)
	}
}

// Full-flow: /tries empty response → attempts list synthesised off
// logCurrentTry, index resolves to current position.
func TestTaskTriesLoaded_EmptyResponseStillPaginable(t *testing.T) {
	t.Parallel()

	m := New(context.Background(), store.New(), &fakeFetcher{}, "http://x", "0")
	m.width, m.height = 120, 30
	m.screen = screenTaskLogs
	m.logDagID, m.logRunID, m.logTaskID = "d", "r", "fails_all_6"
	m.logTryNumber = 6
	m.logCurrentTry = 6
	m.logCurrentState = model.TaskFailed

	next, _ := m.Update(taskTriesLoadedMsg{
		dagID: "d", runID: "r", taskID: "fails_all_6",
		attempts: nil,
	})
	got := next.(Model)

	if len(got.logAttempts) != 6 {
		t.Fatalf("logAttempts len = %d, want 6", len(got.logAttempts))
	}
	if got.logTryIdx != 5 {
		t.Errorf("logTryIdx = %d, want 5", got.logTryIdx)
	}
	if got.logAttempts[5].State != model.TaskFailed {
		t.Errorf("current-try state lost, got %q", got.logAttempts[5].State)
	}
}

// TriesErr fallback still builds a full-range list.
func TestTaskTriesErr_FallsBackToSyntheticRange(t *testing.T) {
	t.Parallel()

	m := New(context.Background(), store.New(), &fakeFetcher{}, "http://x", "0")
	m.width, m.height = 120, 30
	m.screen = screenTaskLogs
	m.logDagID, m.logRunID, m.logTaskID = "d", "r", "fails_all_6"
	m.logTryNumber = 6
	m.logCurrentTry = 6
	m.logCurrentState = model.TaskFailed

	next, _ := m.Update(taskTriesErrMsg{
		dagID: "d", runID: "r", taskID: "fails_all_6",
		err: errStub("HTTP 404"),
	})
	got := next.(Model)

	if len(got.logAttempts) != 6 {
		t.Fatalf("logAttempts len = %d, want 6 after err fallback", len(got.logAttempts))
	}
}

// Off-screen /tries reply must not mutate log state — user has esc'd out.
func TestTaskTriesLoaded_IgnoredOffScreen(t *testing.T) {
	t.Parallel()

	m := New(context.Background(), store.New(), &fakeFetcher{}, "http://x", "0")
	m.screen = screenTaskInstances // ← user is back on the drill list
	m.logDagID, m.logRunID, m.logTaskID = "d", "r", "extract"
	m.logTryNumber = 3
	m.logCurrentTry = 3

	next, _ := m.Update(taskTriesLoadedMsg{
		dagID: "d", runID: "r", taskID: "extract",
		attempts: []model.TaskAttempt{{TryNumber: 1, State: model.TaskFailed}},
	})
	got := next.(Model)
	if got.logAttempts != nil {
		t.Errorf("off-screen tries reply must not populate logAttempts, got %+v", got.logAttempts)
	}
}

type errStub string

func (e errStub) Error() string { return string(e) }

// Switching to a *past* try that's already loaded is instant (cache hit,
// no Cmd fires, no loading state).
func TestSwitchTryIdx_UsesCacheForPastTry(t *testing.T) {
	t.Parallel()

	m := New(context.Background(), store.New(), &fakeFetcher{}, "http://x", "0")
	m.width, m.height = 120, 30
	m.screen = screenTaskLogs
	m.logDagID, m.logRunID, m.logTaskID = "d", "r", "extract"
	m.logAttempts = []model.TaskAttempt{
		{TryNumber: 1, State: model.TaskFailed},
		{TryNumber: 2, State: model.TaskSuccess},
	}
	m.logTryNumber = 2
	m.logTryIdx = 1
	m.logCurrentTry = 2 // the *live* attempt is 2
	m.logContent = "try 2 output"
	m.logCache = map[int]string{
		1: "try 1 output", // past attempt — safe to cache
	}

	next, cmd := m.switchTryIdx(0) // switch to try 1 (past)
	got := next.(Model)
	if cmd != nil {
		t.Errorf("cache hit must not issue a fetch Cmd, got %T", cmd)
	}
	if got.logLoading {
		t.Errorf("cache hit must not enter loading state")
	}
	if got.logContent != "try 1 output" {
		t.Errorf("logContent = %q, want %q (cached)", got.logContent, "try 1 output")
	}
}

// Switching *to* the live attempt must always refetch — its log grows and
// a cached snapshot would silently hide new output.
func TestSwitchTryIdx_LiveTryAlwaysRefetches(t *testing.T) {
	t.Parallel()

	m := New(context.Background(), store.New(), &fakeFetcher{}, "http://x", "0")
	m.width, m.height = 120, 30
	m.screen = screenTaskLogs
	m.logDagID, m.logRunID, m.logTaskID = "d", "r", "extract"
	m.logAttempts = []model.TaskAttempt{
		{TryNumber: 1, State: model.TaskFailed},
		{TryNumber: 2, State: model.TaskRunning},
	}
	m.logTryNumber = 1
	m.logTryIdx = 0
	m.logCurrentTry = 2 // live is try 2
	m.logContent = "try 1 output"
	// Even if the cache somehow ended up with a stale entry for the live
	// try, switchTryIdx must ignore it.
	m.logCache = map[int]string{
		1: "try 1 output",
		2: "stale snapshot of try 2 from earlier",
	}

	next, cmd := m.switchTryIdx(1) // to live try 2
	got := next.(Model)
	if cmd == nil {
		t.Fatal("live-try switch must issue a fetch Cmd")
	}
	if !got.logLoading {
		t.Errorf("live-try switch must enter loading state")
	}
	if got.logContent != "" {
		t.Errorf("live-try switch must clear logContent before refetch, got %q", got.logContent)
	}
}

// Successful load caches past-try content but leaves the live try alone —
// the next switch to live still triggers a refetch.
func TestTaskLogsLoaded_CachesPastTryOnly(t *testing.T) {
	t.Parallel()

	m := New(context.Background(), store.New(), &fakeFetcher{}, "http://x", "0")
	m.width, m.height = 120, 30
	m.screen = screenTaskLogs
	m.logDagID, m.logRunID, m.logTaskID = "d", "r", "extract"
	m.logCurrentTry = 3

	// Response for past try 1 → cache.
	m.logTryNumber = 1
	next, _ := m.Update(taskLogsLoadedMsg{
		dagID: "d", runID: "r", taskID: "extract", tryNumber: 1, content: "old",
	})
	got := next.(Model)
	if cached, ok := got.logCache[1]; !ok || cached != "old" {
		t.Errorf("past-try cache[1] = %q ok=%v, want %q true", cached, ok, "old")
	}

	// Response for live try 3 → NOT cached.
	got.logTryNumber = 3
	next2, _ := got.Update(taskLogsLoadedMsg{
		dagID: "d", runID: "r", taskID: "extract", tryNumber: 3, content: "live",
	})
	got2 := next2.(Model)
	if _, ok := got2.logCache[3]; ok {
		t.Errorf("live-try must not be cached, got cache[3] = %q", got2.logCache[3])
	}
	if got2.logContent != "live" {
		t.Errorf("logContent = %q, want live", got2.logContent)
	}
}

// Late-arriving reply for a superseded try (user already switched away) is
// discarded — no cache write, no panel update.
func TestTaskLogsLoaded_DiscardsStaleTryResponse(t *testing.T) {
	t.Parallel()

	m := New(context.Background(), store.New(), &fakeFetcher{}, "http://x", "0")
	m.width, m.height = 120, 30
	m.screen = screenTaskLogs
	m.logDagID, m.logRunID, m.logTaskID = "d", "r", "extract"
	m.logCurrentTry = 3
	m.logTryNumber = 2     // user is currently viewing try 2
	m.logContent = "try 2" // and its content is loaded
	m.logCache = map[int]string{}

	// Late response for try 3 arrives — must not touch state.
	next, _ := m.Update(taskLogsLoadedMsg{
		dagID: "d", runID: "r", taskID: "extract", tryNumber: 3, content: "try 3",
	})
	got := next.(Model)
	if got.logContent != "try 2" {
		t.Errorf("stale response must not overwrite content, got %q", got.logContent)
	}
	if _, ok := got.logCache[3]; ok {
		t.Errorf("stale response must not populate cache, got cache[3] = %q", got.logCache[3])
	}
}

// Off-screen taskLogsLoadedMsg is discarded — user is not on the log
// viewer anymore, and mutating state would surprise them on re-entry.
func TestTaskLogsLoaded_IgnoredOffScreen(t *testing.T) {
	t.Parallel()

	m := New(context.Background(), store.New(), &fakeFetcher{}, "http://x", "0")
	m.screen = screenDashboard // ← off-screen
	m.logDagID, m.logRunID, m.logTaskID = "d", "r", "extract"
	m.logTryNumber = 3
	m.logCurrentTry = 3

	next, _ := m.Update(taskLogsLoadedMsg{
		dagID: "d", runID: "r", taskID: "extract", tryNumber: 3, content: "leak",
	})
	got := next.(Model)
	if got.logContent == "leak" {
		t.Errorf("off-screen msg must not mutate logContent")
	}
	if _, ok := got.logCache[3]; ok {
		t.Errorf("off-screen msg must not populate cache")
	}
}

// taskLogsErrMsg for a superseded try must not clobber current try's error
// state — previously error for stale try leaked onto content of a
// different try.
func TestTaskLogsErr_IgnoresStaleTry(t *testing.T) {
	t.Parallel()

	m := New(context.Background(), store.New(), &fakeFetcher{}, "http://x", "0")
	m.screen = screenTaskLogs
	m.logDagID, m.logRunID, m.logTaskID = "d", "r", "extract"
	m.logTryNumber = 2  // user on try 2
	m.logContent = "ok" // successfully loaded

	// Late error for try 3 (superseded) arrives.
	next, _ := m.Update(taskLogsErrMsg{
		dagID: "d", runID: "r", taskID: "extract", tryNumber: 3, err: errStub("HTTP 500"),
	})
	got := next.(Model)
	if got.logErr != "" {
		t.Errorf("stale error must not populate logErr, got %q", got.logErr)
	}
}

// Queued task (TryNumber == 0) never fires a fetch and shows a placeholder
// instead of a 404 spinner.
func TestDrillEnterLogs_QueuedTaskShowsPlaceholder(t *testing.T) {
	t.Parallel()

	s := store.New()
	fetcher := &fakeFetcher{}
	m := New(context.Background(), s, fetcher, "http://x", "0")
	m.width, m.height = 120, 30
	m.screen = screenTaskInstances
	m.drillDagID = "d"
	m.drillRunID = "r"
	m.drillItems = []model.TaskInstance{
		{DagID: "d", RunID: "r", TaskID: "not_yet", TryNumber: 0, State: model.TaskQueued},
	}
	m.drillCursor = 0

	next, cmd := m.Update(keyEnter())
	got := next.(Model)
	if got.screen != screenTaskLogs {
		t.Fatalf("screen = %v, want screenTaskLogs", got.screen)
	}
	if got.logLoading {
		t.Errorf("queued task must not enter loading state")
	}
	if cmd != nil {
		t.Errorf("queued task must not issue a fetch Cmd, got %T", cmd)
	}
	if got.logContent == "" {
		t.Errorf("queued task must show a placeholder message, got empty")
	}
}
