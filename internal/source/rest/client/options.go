// Package client wraps the generated Airflow REST client with authentication,
// retries, timeouts and a couple of ergonomic defaults.
package client

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

// Auth carries credentials for the Airflow REST API. Either basic (username +
// password) or bearer (token). Token wins when both are set.
type Auth struct {
	Username string
	Password string
	Token    string
}

// Kind reports which auth flavor will be used.
func (a Auth) Kind() string {
	switch {
	case a.Token != "":
		return "bearer"
	case a.Username != "":
		return "basic"
	default:
		return "none"
	}
}

func (a Auth) validate() error {
	if a.Token == "" && a.Username == "" {
		return errors.New("no credentials: set AIRFLOW_TOKEN or AIRFLOW_USERNAME/AIRFLOW_PASSWORD")
	}
	if a.Token == "" && a.Username != "" && a.Password == "" {
		return errors.New("AIRFLOW_USERNAME set but AIRFLOW_PASSWORD is empty")
	}
	return nil
}

// APIVersion selects which Airflow REST API dialect the client targets. It
// is orthogonal to the Airflow product version — see api/VERSION for the
// specific Airflow releases each generated client is pinned against.
type APIVersion string

// Supported Airflow REST API dialects.
const (
	// APIv1 is the Connexion-based REST API served at /api/v1.
	APIv1 APIVersion = "v1"
	// APIv2 is the FastAPI-based REST API served at /api/v2.
	APIv2 APIVersion = "v2"
)

// Options configures a REST client.
//
// Numeric defaults trigger when the field is zero. In particular MaxRetries
// == 0 is treated as "use the default (3)", not "no retries" — the wrapped
// retryablehttp client has no way to be told "don't retry at all" cleanly,
// and callers who want fewer attempts should pass RetryWaitMin/Max small
// enough that retries finish quickly.
type Options struct {
	BaseURL      string
	Auth         Auth
	Timeout      time.Duration
	MaxRetries   int
	RetryWaitMin time.Duration
	RetryWaitMax time.Duration
	UserAgent    string
	APIVersion   APIVersion
}

// Defaults populates zero fields with reasonable defaults. Does not touch
// BaseURL, Auth or UserAgent — those must come from the caller.
func (o *Options) Defaults() {
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Second
	}
	if o.MaxRetries == 0 {
		o.MaxRetries = 3
	}
	if o.RetryWaitMin == 0 {
		o.RetryWaitMin = 500 * time.Millisecond
	}
	if o.RetryWaitMax == 0 {
		o.RetryWaitMax = 5 * time.Second
	}
	if o.UserAgent == "" {
		o.UserAgent = "afmetric"
	}
	if o.APIVersion == "" {
		o.APIVersion = APIv1
	}
}

// Validate checks that Options can produce a working client.
func (o Options) Validate() error {
	if o.BaseURL == "" {
		return errors.New("BaseURL is empty")
	}
	u, err := url.Parse(o.BaseURL)
	if err != nil {
		return fmt.Errorf("BaseURL is not a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("BaseURL must use http or https, got %q", u.Scheme)
	}
	if o.APIVersion != "" && o.APIVersion != APIv1 && o.APIVersion != APIv2 {
		return fmt.Errorf("APIVersion must be %q or %q, got %q", APIv1, APIv2, o.APIVersion)
	}
	if err := o.Auth.validate(); err != nil {
		return err
	}
	// Airflow REST v2 dropped basic auth in favour of JWT bearer tokens.
	// Catch this misconfiguration at construction time instead of letting
	// the caller collect confusing 401s at runtime.
	if o.APIVersion == APIv2 && o.Auth.Kind() == "basic" {
		return errors.New("APIv2 requires bearer token auth (set AIRFLOW_TOKEN); basic username/password is not supported by Airflow 3.x REST v2")
	}
	return nil
}

// OptionsFromEnv builds Options from AIRFLOW_URL, AIRFLOW_USERNAME,
// AIRFLOW_PASSWORD and AIRFLOW_TOKEN. Missing variables are left empty so the
// caller can layer flags on top before calling Validate.
func OptionsFromEnv() Options {
	return Options{
		BaseURL: strings.TrimRight(os.Getenv("AIRFLOW_URL"), "/"),
		Auth: Auth{
			Username: os.Getenv("AIRFLOW_USERNAME"),
			Password: os.Getenv("AIRFLOW_PASSWORD"),
			Token:    os.Getenv("AIRFLOW_TOKEN"),
		},
	}
}
