package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/client"
	"github.com/duo-moon/airflow-metric-cli/internal/store"
)

func newTestServer(t *testing.T, routes map[string]func(w http.ResponseWriter, r *http.Request)) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for suffix, fn := range routes {
			if strings.HasSuffix(r.URL.Path, suffix) {
				fn(w, r)
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestHealth_PopulatesStore(t *testing.T) {
	t.Parallel()

	url := newTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/health": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{
				"scheduler":{"status":"healthy"},
				"metadatabase":{"status":"healthy"}
			}`))
		},
	})
	c, _ := client.New(client.Options{BaseURL: url, Auth: client.Auth{Token: "t"}})
	s := store.New()

	task := Health(c, s, time.Second)
	if err := task.Fn(context.Background()); err != nil {
		t.Fatalf("Fn: %v", err)
	}

	h := s.Health()
	if !h.AllHealthy() || len(h.Components) != 2 {
		t.Errorf("unexpected health: %+v", h)
	}
}

func TestPools_PopulatesStore(t *testing.T) {
	t.Parallel()

	url := newTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/pools": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{
				"pools":[
					{"name":"default","slots":128,"occupied_slots":10,"running_slots":8,"queued_slots":2,"open_slots":118}
				],
				"total_entries":1
			}`))
		},
	})
	c, _ := client.New(client.Options{BaseURL: url, Auth: client.Auth{Token: "t"}})
	s := store.New()

	if err := Pools(c, s, time.Second).Fn(context.Background()); err != nil {
		t.Fatalf("Fn: %v", err)
	}

	pools := s.Pools()
	if len(pools) != 1 || pools[0].Name != "default" || pools[0].Slots != 128 {
		t.Errorf("unexpected pools: %+v", pools)
	}
}

func TestDagRuns_PopulatesStore(t *testing.T) {
	t.Parallel()

	// Anchor timestamps to real "now" so the RecentFailedDagRunsSince
	// window covers the failed run even as we cross calendar boundaries.
	nowUTC := time.Now().UTC()
	startRunning := nowUTC.Add(-30 * time.Minute).Format(time.RFC3339)
	startFailed := nowUTC.Add(-2 * time.Hour).Format(time.RFC3339)
	endFailed := nowUTC.Add(-90 * time.Minute).Format(time.RFC3339)

	body := fmt.Sprintf(`{
		"dag_runs":[
			{"dag_id":"etl","dag_run_id":"r-1","state":"running","start_date":%q},
			{"dag_id":"etl","dag_run_id":"r-2","state":"failed","start_date":%q,"end_date":%q}
		],
		"total_entries":2
	}`, startRunning, startFailed, endFailed)

	url := newTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/dags/~/dagRuns/list": func(w http.ResponseWriter, r *http.Request) {
			reqBody, _ := readBody(r)
			if !strings.Contains(reqBody, `"states"`) {
				t.Errorf("expected states filter, got body=%s", reqBody)
			}
			_, _ = w.Write([]byte(body))
		},
	})
	c, _ := client.New(client.Options{BaseURL: url, Auth: client.Auth{Token: "t"}})
	s := store.New()

	if err := DagRuns(c, s, time.Second, 50).Fn(context.Background()); err != nil {
		t.Fatalf("Fn: %v", err)
	}

	if s.DagRunCount() != 2 {
		t.Fatalf("count = %d, want 2", s.DagRunCount())
	}
	if got := s.ActiveDagRuns(); len(got) != 1 || got[0].RunID != "r-1" {
		t.Errorf("expected active r-1, got %+v", got)
	}
	if got := s.RecentFailedDagRunsSince(nowUTC, 24*time.Hour); len(got) != 1 || got[0].RunID != "r-2" {
		t.Errorf("expected failed r-2, got %+v", got)
	}
}

func TestHealth_NonOKReturnsError(t *testing.T) {
	t.Parallel()

	url := newTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/health": func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "nope", http.StatusServiceUnavailable)
		},
	})
	c, _ := client.New(client.Options{
		BaseURL:      url,
		Auth:         client.Auth{Token: "t"},
		RetryWaitMin: time.Millisecond,
		RetryWaitMax: 2 * time.Millisecond,
	})
	s := store.New()

	err := Health(c, s, time.Second).Fn(context.Background())
	if err == nil {
		t.Fatal("expected error on 503")
	}
}

func TestImportErrors_PopulatesStore(t *testing.T) {
	t.Parallel()

	url := newTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/importErrors": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{
				"import_errors":[
					{"import_error_id":1,"filename":"dags/bad.py","stack_trace":"Traceback...","timestamp":"2026-09-16T10:00:00Z"},
					{"import_error_id":2,"filename":"dags/worse.py","stack_trace":"NameError","timestamp":"2026-09-16T11:00:00Z"}
				],
				"total_entries":2
			}`))
		},
	})
	c, _ := client.New(client.Options{BaseURL: url, Auth: client.Auth{Token: "t"}})
	s := store.New()

	if err := ImportErrors(c, s, time.Second).Fn(context.Background()); err != nil {
		t.Fatalf("Fn: %v", err)
	}

	errs := s.ImportErrors()
	if len(errs) != 2 {
		t.Fatalf("len = %d, want 2", len(errs))
	}
	// Store sorts newest-first.
	if errs[0].ID != 2 || errs[0].Filename != "dags/worse.py" {
		t.Errorf("expected newest first, got %+v", errs[0])
	}
}

func TestWaitingTasks_AggregatesByRun(t *testing.T) {
	t.Parallel()

	url := newTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/dags/~/dagRuns/~/taskInstances/list": func(w http.ResponseWriter, r *http.Request) {
			body, _ := readBody(r)
			if !strings.Contains(body, `"state"`) {
				t.Errorf("expected state filter in body, got %s", body)
			}
			_, _ = w.Write([]byte(`{
				"task_instances":[
					{"dag_id":"etl","dag_run_id":"r-1","task_id":"a","state":"queued"},
					{"dag_id":"etl","dag_run_id":"r-1","task_id":"b","state":"up_for_retry"},
					{"dag_id":"reports","dag_run_id":"r-9","task_id":"c","state":"deferred"}
				],
				"total_entries":3
			}`))
		},
	})
	c, _ := client.New(client.Options{BaseURL: url, Auth: client.Auth{Token: "t"}})
	s := store.New()

	if err := WaitingTasks(c, s, time.Second, 100).Fn(context.Background()); err != nil {
		t.Fatalf("Fn: %v", err)
	}

	etl := s.WaitingCount("etl", "r-1")
	if etl.Queued != 1 || etl.Retry != 1 || etl.Total() != 2 {
		t.Errorf("etl aggregation wrong: %+v", etl)
	}
	reports := s.WaitingCount("reports", "r-9")
	if reports.Deferred != 1 || reports.Total() != 1 {
		t.Errorf("reports aggregation wrong: %+v", reports)
	}
}

func TestPruner_DropsExpiredTerminalRuns(t *testing.T) {
	t.Parallel()

	now := time.Now()
	s := store.New()
	s.UpsertDagRuns([]model.DagRun{
		{DagID: "d", RunID: "old-terminal", State: model.DagRunSuccess, UpdatedAt: now.Add(-24 * time.Hour)},
		{DagID: "d", RunID: "young-terminal", State: model.DagRunFailed, UpdatedAt: now.Add(-10 * time.Minute)},
		{DagID: "d", RunID: "old-running", State: model.DagRunRunning, UpdatedAt: now.Add(-24 * time.Hour)},
	})

	// TTL 1h — old-terminal must go; old-running is kept regardless of age;
	// young-terminal is inside the window.
	if err := Pruner(s, time.Hour, time.Second).Fn(context.Background()); err != nil {
		t.Fatalf("Fn: %v", err)
	}

	if s.DagRunCount() != 2 {
		t.Errorf("count = %d, want 2 (old-terminal should be gone)", s.DagRunCount())
	}
	for _, r := range s.ActiveDagRuns() {
		if r.RunID != "old-running" {
			continue
		}
		return
	}
	t.Error("old-running non-terminal must survive pruning")
}

func readBody(r *http.Request) (string, error) {
	var buf strings.Builder
	dec := json.NewDecoder(r.Body)
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	return buf.String(), nil
}
