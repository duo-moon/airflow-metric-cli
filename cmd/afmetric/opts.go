package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/client"
	"github.com/duo-moon/airflow-metric-cli/internal/version"
)

// wrapConfigError turns terse client.New / Validate errors into something a
// first-time user can act on. Only the common "BaseURL is empty" case gets
// an extra hint; everything else is passed through unchanged.
func wrapConfigError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "BaseURL is empty") {
		return fmt.Errorf("%w\n\nSet AIRFLOW_URL or pass --url, e.g.:\n  AIRFLOW_URL=https://airflow.example.com AIRFLOW_TOKEN=... afmetric ping", err)
	}
	return err
}

// buildClientOptions merges (in priority order, highest wins) CLI flags →
// AIRFLOW_* env vars into a client.Options ready to hand to client.New.
// Multi-cluster setups are expected to be handled outside of afmetric —
// a shell alias or wrapper script is enough.
func buildClientOptions(cmd *cobra.Command, flagBaseURL string, flagTimeout time.Duration) client.Options {
	flagAPIVersion, _ := cmd.Flags().GetString("api-version")

	opts := client.OptionsFromEnv()

	if flagBaseURL != "" {
		opts.BaseURL = strings.TrimRight(flagBaseURL, "/")
	}
	if flagTimeout > 0 {
		opts.Timeout = flagTimeout
	}
	if flagAPIVersion != "" {
		opts.APIVersion = client.APIVersion(flagAPIVersion)
	}

	opts.UserAgent = client.UserAgent(version.Version)
	return opts
}
