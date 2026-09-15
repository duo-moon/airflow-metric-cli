// Package collector wires the version-independent AirflowClient interface,
// the store and the poller together into poller.Task values ready for
// registration.
package collector

import (
	"context"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/poller"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/client"
	"github.com/duo-moon/airflow-metric-cli/internal/store"
)

// Health polls /health and refreshes ClusterHealth.
func Health(cli client.AirflowClient, s *store.Store, interval time.Duration) poller.Task {
	return poller.Task{
		Name:     "health",
		Interval: interval,
		Jitter:   jitter(interval),
		Fn: func(ctx context.Context) error {
			h, err := cli.Health(ctx)
			if err != nil {
				return err
			}
			s.UpsertHealth(h)
			return nil
		},
	}
}

// Pools polls /pools and replaces the current pool set.
func Pools(cli client.AirflowClient, s *store.Store, interval time.Duration) poller.Task {
	return poller.Task{
		Name:     "pools",
		Interval: interval,
		Jitter:   jitter(interval),
		Fn: func(ctx context.Context) error {
			p, err := cli.Pools(ctx)
			if err != nil {
				return err
			}
			s.UpsertPools(p)
			return nil
		},
	}
}

// ImportErrors polls /importErrors.
func ImportErrors(cli client.AirflowClient, s *store.Store, interval time.Duration) poller.Task {
	return poller.Task{
		Name:     "import_errors",
		Interval: interval,
		Jitter:   jitter(interval),
		Fn: func(ctx context.Context) error {
			e, err := cli.ImportErrors(ctx)
			if err != nil {
				return err
			}
			s.UpsertImportErrors(e)
			return nil
		},
	}
}

// DagRuns polls the batched dagRuns endpoint asking for every non-terminal
// state plus success/failed, then merges results into the store. Terminal
// runs age out via Pruner.
func DagRuns(cli client.AirflowClient, s *store.Store, interval time.Duration, pageLimit int) poller.Task {
	states := []model.DagRunState{
		model.DagRunQueued,
		model.DagRunRunning,
		model.DagRunSuccess,
		model.DagRunFailed,
	}
	return poller.Task{
		Name:     "dag_runs",
		Interval: interval,
		Jitter:   jitter(interval),
		Fn: func(ctx context.Context) error {
			runs, err := cli.DagRuns(ctx, states, pageLimit)
			if err != nil {
				return err
			}
			s.UpsertDagRuns(runs)
			return nil
		},
	}
}

// WaitingTasks polls task instances in waiting states across every DAG run
// via one batched request and pushes aggregated counts into the store.
func WaitingTasks(cli client.AirflowClient, s *store.Store, interval time.Duration, pageLimit int) poller.Task {
	return poller.Task{
		Name:     "waiting_tasks",
		Interval: interval,
		Jitter:   jitter(interval),
		Fn: func(ctx context.Context) error {
			counts, err := cli.WaitingTasks(ctx, pageLimit)
			if err != nil {
				return err
			}
			s.UpsertWaitingCounts(counts)
			return nil
		},
	}
}

// Pruner returns a task that drops terminal DAG runs older than ttl.
func Pruner(s *store.Store, ttl, interval time.Duration) poller.Task {
	return poller.Task{
		Name:     "prune",
		Interval: interval,
		Fn: func(_ context.Context) error {
			s.PruneDagRunsBefore(time.Now().Add(-ttl))
			return nil
		},
	}
}

func jitter(interval time.Duration) time.Duration {
	j := interval / 10
	if j < 100*time.Millisecond {
		j = 100 * time.Millisecond
	}
	return j
}
