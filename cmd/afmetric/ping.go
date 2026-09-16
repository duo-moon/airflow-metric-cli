package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/client"
)

func newPingCmd() *cobra.Command {
	var (
		baseURL string
		timeout time.Duration
	)

	cmd := &cobra.Command{
		Use:   "ping",
		Short: "Check connectivity and print Airflow version + component health",
		RunE: func(cmd *cobra.Command, _ []string) error {
			opts := buildClientOptions(cmd, baseURL, timeout)
			c, err := client.New(opts)
			if err != nil {
				return wrapConfigError(err)
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), timeout+5*time.Second)
			defer cancel()

			return runPing(ctx, c, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVar(&baseURL, "url", "", "Airflow base URL (default: $AIRFLOW_URL)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "Per-request timeout")

	return cmd
}

func runPing(ctx context.Context, c client.AirflowClient, out io.Writer) error {
	version, git, err := c.Version(ctx)
	if err != nil {
		return fmt.Errorf("version: %w", err)
	}

	h, err := c.Health(ctx)
	if err != nil {
		return fmt.Errorf("health: %w", err)
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "Airflow\t%s\n", nonEmpty(version, "n/a"))
	_, _ = fmt.Fprintf(tw, "Git\t%s\n", nonEmpty(git, "n/a"))
	for _, comp := range h.Components {
		_, _ = fmt.Fprintf(tw, "%s\t%s\n", comp.Name, comp.Status)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if bad := h.Unhealthy(); len(bad) > 0 {
		names := make([]string, 0, len(bad))
		for _, c := range bad {
			names = append(names, c.Name)
		}
		return fmt.Errorf("%d of %d component(s) unhealthy: %s",
			len(bad), len(h.Components), strings.Join(names, ", "))
	}
	return nil
}

func nonEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
