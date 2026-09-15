package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew_BasicAuthAndUserAgent(t *testing.T) {
	t.Parallel()

	var (
		gotAuth string
		gotUA   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"2.10.5","git_version":"abc"}`))
	}))
	t.Cleanup(srv.Close)

	c, err := New(Options{
		BaseURL:   srv.URL,
		Auth:      Auth{Username: "alice", Password: "secret"},
		UserAgent: "afmetric/test",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, _, err := c.Version(context.Background()); err != nil {
		t.Fatalf("Version: %v", err)
	}

	// base64("alice:secret") == "YWxpY2U6c2VjcmV0"
	if want := "Basic YWxpY2U6c2VjcmV0"; gotAuth != want {
		t.Errorf("Authorization = %q, want %q", gotAuth, want)
	}
	if gotUA != "afmetric/test" {
		t.Errorf("User-Agent = %q, want afmetric/test", gotUA)
	}
}

func TestNew_BearerBeatsBasic(t *testing.T) {
	t.Parallel()

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"2.10.5"}`))
	}))
	t.Cleanup(srv.Close)

	c, err := New(Options{
		BaseURL: srv.URL,
		Auth:    Auth{Username: "alice", Password: "secret", Token: "tok-42"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, _, err := c.Version(context.Background()); err != nil {
		t.Fatalf("Version: %v", err)
	}

	if gotAuth != "Bearer tok-42" {
		t.Errorf("Authorization = %q, want Bearer tok-42", gotAuth)
	}
}

func TestNew_RetriesOn5xx(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"2.10.5"}`))
	}))
	t.Cleanup(srv.Close)

	c, err := New(Options{
		BaseURL:      srv.URL,
		Auth:         Auth{Token: "t"},
		MaxRetries:   3,
		RetryWaitMin: 1 * time.Millisecond,
		RetryWaitMax: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, _, err := c.Version(context.Background()); err != nil {
		t.Fatalf("Version: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3 (2 failures + 1 success)", calls.Load())
	}
}

func TestNew_AppendsAPIV1Prefix(t *testing.T) {
	t.Parallel()

	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"2.10.5"}`))
	}))
	t.Cleanup(srv.Close)

	c, err := New(Options{BaseURL: srv.URL, Auth: Auth{Token: "t"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, _, err := c.Version(context.Background()); err != nil {
		t.Fatalf("Version: %v", err)
	}
	if gotPath != "/api/v1/version" {
		t.Errorf("path = %q, want /api/v1/version", gotPath)
	}
}

func TestWithAPIPrefix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		v    APIVersion
		want string
	}{
		{"http://x", "", "http://x/api/v1"},
		{"http://x/", APIv1, "http://x/api/v1"},
		{"http://x/api/v1", APIv1, "http://x/api/v1"},
		{"http://x/api/v1/", APIv1, "http://x/api/v1"},
		// v2 generated client already puts /api/v2 into operationPath —
		// we must NOT double-prefix.
		{"http://x", APIv2, "http://x"},
		{"http://x/api/v2", APIv2, "http://x"},
		{"http://x/api/v2/", APIv2, "http://x"},
	}
	for _, c := range cases {
		if got := withAPIPrefix(c.in, c.v); got != c.want {
			t.Errorf("withAPIPrefix(%q, %q) = %q, want %q", c.in, c.v, got, c.want)
		}
	}
}

func TestNew_ValidationErrors(t *testing.T) {
	t.Parallel()

	_, err := New(Options{BaseURL: "http://x"})
	if err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Fatalf("expected credentials error, got %v", err)
	}
}
