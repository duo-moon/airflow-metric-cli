package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"

	"github.com/duo-moon/airflow-metric-cli/internal/collector"
	"github.com/duo-moon/airflow-metric-cli/internal/poller"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/client"
	"github.com/duo-moon/airflow-metric-cli/internal/store"
	"github.com/duo-moon/airflow-metric-cli/internal/ui"
	"github.com/duo-moon/airflow-metric-cli/internal/version"
)

// runFlags collects everything the run command exposes on the command line.
type runFlags struct {
	baseURL          string
	timeout          time.Duration
	fastInterval     time.Duration
	slowInterval     time.Duration
	dagrunTTL        time.Duration
	dagrunLimit      int
	noGuard          bool
	guardMaxInterval time.Duration
	guardTick        time.Duration
}

func newRunCmd() *cobra.Command {
	var f runFlags

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Launch the TUI dashboard",
		RunE: func(cmd *cobra.Command, _ []string) error {
			opts := buildClientOptions(cmd, f.baseURL, f.timeout)
			c, err := client.New(opts)
			if err != nil {
				return wrapConfigError(err)
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			s := store.New()
			p := poller.New()
			p.Add(collector.Health(c, s, f.fastInterval))
			p.Add(collector.DagRuns(c, s, f.fastInterval, f.dagrunLimit))
			p.Add(collector.WaitingTasks(c, s, f.fastInterval, 500))
			p.Add(collector.Pools(c, s, f.slowInterval))
			p.Add(collector.ImportErrors(c, s, f.slowInterval))
			p.Add(collector.Pruner(s, f.dagrunTTL, 5*time.Minute))
			addGuard(p, f)

			pollerDone := make(chan error, 1)
			go func() { pollerDone <- p.Run(ctx) }()

			m := ui.New(ctx, s, c, opts.BaseURL, version.Version)
			prog := tea.NewProgram(m, tea.WithAltScreen(), tea.WithContext(ctx))

			_, runErr := prog.Run()
			stop()
			<-pollerDone
			return runErr
		},
	}

	cmd.Flags().StringVar(&f.baseURL, "url", "", "Airflow base URL (default: $AIRFLOW_URL)")
	cmd.Flags().DurationVar(&f.timeout, "timeout", 10*time.Second, "Per-request timeout")
	cmd.Flags().DurationVar(&f.fastInterval, "interval", 5*time.Second, "Poll interval for health and DAG runs")
	cmd.Flags().DurationVar(&f.slowInterval, "slow-interval", 30*time.Second, "Poll interval for pools and import errors")
	cmd.Flags().DurationVar(&f.dagrunTTL, "dagrun-ttl", 24*time.Hour, "Drop terminal DAG runs older than this")
	cmd.Flags().IntVar(&f.dagrunLimit, "dagrun-limit", 100, "Max DAG runs to fetch per poll")
	cmd.Flags().BoolVar(&f.noGuard, "no-guard", false, "Disable adaptive rate-limit guard")
	cmd.Flags().DurationVar(&f.guardMaxInterval, "guard-max", 2*time.Minute, "Max interval a task can be throttled to")
	cmd.Flags().DurationVar(&f.guardTick, "guard-tick", 30*time.Second, "How often the guard reassesses error rate")

	return cmd
}

// addGuard wires a rate-limit Guard into the poller. When errors climb the
// Guard doubles each watched task's interval (up to guardMaxInterval); when
// they clear it dials things back down to the baseline.
func addGuard(p *poller.Poller, f runFlags) {
	if f.noGuard {
		return
	}
	g := poller.NewGuard(p, f.guardMaxInterval, 0.5)
	g.Watch("health", f.fastInterval)
	g.Watch("dag_runs", f.fastInterval)
	g.Watch("pools", f.slowInterval)
	g.Watch("import_errors", f.slowInterval)
	p.Add(g.Task(f.guardTick))
}
