package client

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/hashicorp/go-retryablehttp"

	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/airflowv1"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/airflowv2"
)

// apiPathV1 / apiPathV2 mirror the `servers.url` in the pinned Airflow
// OpenAPI specs. oapi-codegen does not inject them into generated request
// paths, so callers must append them to the base URL themselves.
const (
	apiPathV1 = "/api/v1"
	apiPathV2 = "/api/v2"
)

// requestEditor mirrors the two generated packages' RequestEditorFn shape,
// which happens to be identical. Kept local so we can pass the same auth /
// UA hooks to either generated client.
type requestEditor func(ctx context.Context, req *http.Request) error

// New builds an AirflowClient for the given options. The concrete adapter
// is selected by opts.APIVersion — see APIv1 / APIv2.
func New(opts Options) (AirflowClient, error) {
	opts.Defaults()
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	httpClient := newRetryingHTTPClient(opts)
	baseURL := withAPIPrefix(opts.BaseURL, opts.APIVersion)
	auth := authEditor(opts.Auth)
	ua := userAgentEditor(opts.UserAgent)

	if opts.APIVersion == APIv2 {
		c, err := airflowv2.NewClientWithResponses(baseURL,
			airflowv2.WithHTTPClient(httpClient),
			airflowv2.WithRequestEditorFn(airflowv2.RequestEditorFn(auth)),
			airflowv2.WithRequestEditorFn(airflowv2.RequestEditorFn(ua)),
		)
		if err != nil {
			return nil, err
		}
		return newV2Adapter(c), nil
	}

	c, err := airflowv1.NewClientWithResponses(baseURL,
		airflowv1.WithHTTPClient(httpClient),
		airflowv1.WithRequestEditorFn(airflowv1.RequestEditorFn(auth)),
		airflowv1.WithRequestEditorFn(airflowv1.RequestEditorFn(ua)),
	)
	if err != nil {
		return nil, err
	}
	return newV1Adapter(c), nil
}

// withAPIPrefix normalises baseURL to what each generated client expects.
//
//   - v1 spec declares servers.url = "/api/v1", but oapi-codegen does NOT
//     inject it into request paths — so we append /api/v1 here.
//   - v2 spec has no servers.url and every generated operationPath already
//     starts with "/api/v2/...". Passing a base URL that also ends in
//     /api/v2 would double-prefix, so we strip it if present.
//
// Both branches tolerate trailing slashes.
func withAPIPrefix(baseURL string, v APIVersion) string {
	trimmed := strings.TrimRight(baseURL, "/")
	if v == APIv2 {
		return strings.TrimSuffix(trimmed, apiPathV2)
	}
	if strings.HasSuffix(trimmed, apiPathV1) {
		return trimmed
	}
	return trimmed + apiPathV1
}

func newRetryingHTTPClient(opts Options) *http.Client {
	rc := retryablehttp.NewClient()
	rc.RetryMax = opts.MaxRetries
	rc.RetryWaitMin = opts.RetryWaitMin
	rc.RetryWaitMax = opts.RetryWaitMax
	rc.Logger = nil // keep stdout/stderr clean; failures surface via returned errors

	httpClient := rc.StandardClient()
	httpClient.Timeout = opts.Timeout
	return httpClient
}

func authEditor(a Auth) requestEditor {
	return func(_ context.Context, req *http.Request) error {
		switch {
		case a.Token != "":
			req.Header.Set("Authorization", "Bearer "+a.Token)
		case a.Username != "":
			cred := a.Username + ":" + a.Password
			req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(cred)))
		}
		return nil
	}
}

func userAgentEditor(ua string) requestEditor {
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("User-Agent", ua)
		return nil
	}
}

// UserAgent returns the canonical User-Agent string for a given afmetric build.
func UserAgent(version string) string {
	return fmt.Sprintf("afmetric/%s (+https://github.com/duo-moon/airflow-metric-cli)", version)
}
