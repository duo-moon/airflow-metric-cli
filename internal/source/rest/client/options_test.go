package client

import (
	"strings"
	"testing"
	"time"
)

func TestAuthKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth Auth
		want string
	}{
		{"token wins over basic", Auth{Username: "u", Password: "p", Token: "t"}, "bearer"},
		{"basic only", Auth{Username: "u", Password: "p"}, "basic"},
		{"empty", Auth{}, "none"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.auth.Kind(); got != tc.want {
				t.Fatalf("Kind() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOptionsValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    Options
		wantErr string
	}{
		{
			name:    "empty BaseURL",
			opts:    Options{Auth: Auth{Token: "t"}},
			wantErr: "BaseURL is empty",
		},
		{
			name:    "bad scheme",
			opts:    Options{BaseURL: "ftp://x", Auth: Auth{Token: "t"}},
			wantErr: "must use http or https",
		},
		{
			name:    "no credentials",
			opts:    Options{BaseURL: "http://x"},
			wantErr: "no credentials",
		},
		{
			name:    "username without password",
			opts:    Options{BaseURL: "http://x", Auth: Auth{Username: "u"}},
			wantErr: "AIRFLOW_PASSWORD is empty",
		},
		{
			name: "bearer ok",
			opts: Options{BaseURL: "http://x", Auth: Auth{Token: "t"}},
		},
		{
			name: "basic ok",
			opts: Options{BaseURL: "https://x", Auth: Auth{Username: "u", Password: "p"}},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.opts.Validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestOptionsDefaults(t *testing.T) {
	t.Parallel()

	var o Options
	o.Defaults()

	if o.Timeout != 10*time.Second {
		t.Errorf("Timeout = %v, want 10s", o.Timeout)
	}
	if o.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", o.MaxRetries)
	}
	if o.UserAgent != "afmetric" {
		t.Errorf("UserAgent = %q, want %q", o.UserAgent, "afmetric")
	}

	// Defaults must not overwrite user-set values.
	o2 := Options{Timeout: 42 * time.Second, MaxRetries: 7, UserAgent: "custom"}
	o2.Defaults()
	if o2.Timeout != 42*time.Second || o2.MaxRetries != 7 || o2.UserAgent != "custom" {
		t.Errorf("Defaults() overwrote user-provided values: %+v", o2)
	}
}

func TestOptionsFromEnv(t *testing.T) {
	t.Setenv("AIRFLOW_URL", "https://airflow.example.com/")
	t.Setenv("AIRFLOW_USERNAME", "alice")
	t.Setenv("AIRFLOW_PASSWORD", "secret")
	t.Setenv("AIRFLOW_TOKEN", "")

	got := OptionsFromEnv()
	if got.BaseURL != "https://airflow.example.com" {
		t.Errorf("BaseURL = %q, want trailing slash trimmed", got.BaseURL)
	}
	if got.Auth.Username != "alice" || got.Auth.Password != "secret" {
		t.Errorf("Auth = %+v, want alice/secret", got.Auth)
	}
	if got.Auth.Token != "" {
		t.Errorf("Token = %q, want empty", got.Auth.Token)
	}
}
