package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/client"
)

func TestRunPing_HealthyCluster(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/version"):
			_, _ = w.Write([]byte(`{"version":"2.10.5","git_version":"abc123"}`))
		case strings.HasSuffix(r.URL.Path, "/health"):
			_, _ = w.Write([]byte(`{
				"scheduler":{"status":"healthy","latest_scheduler_heartbeat":"2026-08-17T18:00:00Z"},
				"metadatabase":{"status":"healthy"},
				"triggerer":{"status":"healthy","latest_triggerer_heartbeat":"2026-08-17T18:00:00Z"}
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := client.New(client.Options{BaseURL: srv.URL, Auth: client.Auth{Token: "t"}})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	var out bytes.Buffer
	if err := runPing(context.Background(), c, &out); err != nil {
		t.Fatalf("runPing: %v", err)
	}

	got := out.String()
	for _, want := range []string{"Airflow", "2.10.5", "Scheduler", "healthy", "Triggerer"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q; got:\n%s", want, got)
		}
	}
}

func TestRunPing_UnhealthyScheduler(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/version"):
			_, _ = w.Write([]byte(`{"version":"2.10.5"}`))
		case strings.HasSuffix(r.URL.Path, "/health"):
			_, _ = w.Write([]byte(`{
				"scheduler":{"status":"unhealthy"},
				"metadatabase":{"status":"healthy"}
			}`))
		}
	}))
	t.Cleanup(srv.Close)

	c, _ := client.New(client.Options{BaseURL: srv.URL, Auth: client.Auth{Token: "t"}})

	err := runPing(context.Background(), c, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "unhealthy") {
		t.Fatalf("expected unhealthy error, got %v", err)
	}
}
