package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/airflowv2"
)

// newV2TestClient wires client.New with a real httptest.Server and APIv2.
func newV2TestClient(t *testing.T, h http.Handler) (AirflowClient, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	c, err := New(Options{
		BaseURL:    srv.URL,
		APIVersion: APIv2,
		Auth:       Auth{Token: "tok-42"},
	})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return c, srv
}

func TestV2_VersionUsesBearerAndV2Prefix(t *testing.T) {
	t.Parallel()

	var (
		gotPath string
		gotAuth string
		gotUA   string
	)
	c, _ := newV2TestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"3.0.5","git_version":"abc"}`))
	}))

	ver, git, err := c.Version(context.Background())
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if ver != "3.0.5" || git != "abc" {
		t.Errorf("Version() = (%q, %q)", ver, git)
	}
	if gotPath != "/api/v2/version" {
		t.Errorf("path = %q, want /api/v2/version", gotPath)
	}
	if gotAuth != "Bearer tok-42" {
		t.Errorf("Authorization = %q, want Bearer tok-42", gotAuth)
	}
	if !strings.HasPrefix(gotUA, "afmetric") {
		t.Errorf("User-Agent = %q, want prefix afmetric", gotUA)
	}
}

func TestV2_Health(t *testing.T) {
	t.Parallel()

	c, _ := newV2TestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"scheduler":    {"status": "healthy", "latest_scheduler_heartbeat": "2026-08-17T18:00:00Z"},
			"metadatabase": {"status": "healthy"},
			"triggerer":    {"status": "unhealthy", "latest_triggerer_heartbeat": null}
		}`))
	}))

	h, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	byName := map[string]model.HealthStatus{}
	for _, c := range h.Components {
		byName[c.Name] = c.Status
	}
	if byName["Scheduler"] != model.HealthHealthy || byName["Triggerer"] != model.HealthUnhealthy {
		t.Errorf("statuses wrong: %+v", byName)
	}
	if _, ok := byName["DagProcessor"]; ok {
		t.Errorf("DagProcessor should be absent when key missing, got %+v", byName)
	}
}

func TestV2_DagRunsBatchAndWildcard(t *testing.T) {
	t.Parallel()

	var (
		gotPath   string
		gotStates []string
	)
	c, _ := newV2TestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		// Decode body to inspect the states filter.
		var body struct {
			States []string `json:"states"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotStates = body.States

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"dag_runs": [
				{"dag_id":"d","dag_run_id":"r","state":"running","run_after":"2026-08-17T10:00:00Z","dag_versions":[]}
			],
			"total_entries": 1
		}`))
	}))

	got, err := c.DagRuns(context.Background(), []model.DagRunState{model.DagRunRunning, model.DagRunFailed}, 50)
	if err != nil {
		t.Fatalf("DagRuns: %v", err)
	}
	if gotPath != "/api/v2/dags/~/dagRuns/list" {
		t.Errorf("path = %q, want batch list with ~", gotPath)
	}
	if len(gotStates) != 2 || gotStates[0] != "running" || gotStates[1] != "failed" {
		t.Errorf("states filter = %v, want [running, failed]", gotStates)
	}
	if len(got) != 1 || got[0].DagID != "d" || got[0].State != model.DagRunRunning {
		t.Errorf("mapped runs = %+v, want single running run", got)
	}
}

func TestV2_ValidateRejectsBasicAuth(t *testing.T) {
	t.Parallel()

	_, err := New(Options{
		BaseURL:    "http://x",
		APIVersion: APIv2,
		Auth:       Auth{Username: "u", Password: "p"},
	})
	if err == nil || !strings.Contains(err.Error(), "APIv2 requires bearer") {
		t.Fatalf("expected APIv2+basic to be rejected, got %v", err)
	}
}

func TestDecodeV2LogContent_StructuredMessages(t *testing.T) {
	t.Parallel()

	// Union.From... helpers on the generated type let us build a real union
	// value without hand-crafting json.RawMessage.
	msgs := airflowv2.TaskInstancesLogResponseContent0{
		{Event: "starting", Timestamp: ptrTime("2026-08-17T18:00:00Z")},
		{Event: "boom", AdditionalProperties: map[string]any{"level": "ERROR"}},
	}
	var u airflowv2.TaskInstancesLogResponse_Content
	if err := u.FromTaskInstancesLogResponseContent0(msgs); err != nil {
		t.Fatalf("prep union: %v", err)
	}

	got := decodeV2LogContent(&u)
	// Expect timestamp + event on the first line, event + extras on the second.
	for _, want := range []string{"2026-08-17T18:00:00Z", "starting", "boom", "level=", `"ERROR"`} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in decoded output:\n%s", want, got)
		}
	}
	if strings.Count(got, "\n") != 1 {
		t.Errorf("expected 2 lines joined by 1 newline, got:\n%s", got)
	}
}

func TestDecodeV2LogContent_PlainStrings(t *testing.T) {
	t.Parallel()

	lines := airflowv2.TaskInstancesLogResponseContent1{"a", "b", "c"}
	var u airflowv2.TaskInstancesLogResponse_Content
	if err := u.FromTaskInstancesLogResponseContent1(lines); err != nil {
		t.Fatalf("prep union: %v", err)
	}

	if got := decodeV2LogContent(&u); got != "a\nb\nc" {
		t.Errorf("plain-strings decode = %q, want a\\nb\\nc", got)
	}
}

func TestDecodeV2LogContent_Empty(t *testing.T) {
	t.Parallel()

	// Uninitialised union — no branch populated.
	var u airflowv2.TaskInstancesLogResponse_Content
	if got := decodeV2LogContent(&u); got != "" {
		t.Errorf("empty union should decode to empty string, got %q", got)
	}
}

// ptrTime returns *time.Time parsed from an RFC3339 string. Test helper.
func ptrTime(s string) *time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return &t
}
