# Demo DAGs for afmetric

Bind-mounted at `/opt/airflow/dags` by `demo/docker-compose.yaml` — edit any
file here and the scheduler picks it up within `DAG_DIR_LIST_INTERVAL`
(default 30s).

Each DAG targets a specific corner of the afmetric TUI:

| File                    | State it produces                                    | Panel it feeds                          |
|-------------------------|------------------------------------------------------|-----------------------------------------|
| `healthy_pipeline.py`   | success                                              | Active Runs (baseline traffic)          |
| `flaky_pipeline.py`     | `up_for_retry` → success on try 4                    | (try N/M) counter, WAIT column          |
| `multi_try_pipeline.py` | 3 parallel tasks ending at try 2 / try 4 / try 6     | log-viewer `[ / ]` navigation showcase  |
| `always_fails.py`       | `failed`                                             | Recent Failures, red ERROR in logs      |
| `long_running.py`       | `running` for ~5 min                                 | Active Runs duration, log scrolling     |
| `chatty_logs.py`        | success + hundreds of mixed-level log lines          | Log viewer colorize + `/search`         |
| `sensor_wait.py`        | `up_for_reschedule`                                  | WAIT column, sensor state               |
| `deferred_wait.py`      | `deferred`                                           | WAIT column, triggerer health           |
| `pool_heavy.py`         | `queued` + `running` fighting over 2 slots           | Pools panel (utilisation), WAIT column  |
| `skip_chain.py`         | `skipped` + `upstream_failed`                        | drill-down task instances               |
| `broken_dag.py`         | parse error at import                                | Import Errors badge in header           |

Any custom pools referenced above (`heavy_pool`) are created by the
`airflow-init` container.

## Bring the whole thing up

Two flavours are wired via docker-compose profiles — pick one, they
share the same DAG bind-mount but cannot run at the same time (they
fight over the same postgres schema).

```
# Airflow 2.x (default, /api/v1, basic auth)
make demo-up          # ~3 min on first run (image + migrations)
make demo-run         # launches the TUI

# Airflow 3.x (/api/v2, JWT via /auth/token)
make demo-up-v3
make demo-run-v3      # fetches a JWT with admin/admin and passes it inline
```

Tear down with `make demo-down` or `make demo-down-v3`.

Then in the TUI:
- Watch the counters climb; every 2–15 min you should see runs from each
  DAG show up somewhere.
- Enter `pool_heavy` in Active Runs to see queued vs running tasks.
- Enter `flaky_pipeline` while it's in `up_for_retry` — the (try N/M)
  counter turns yellow with a bold accent when you page backwards with `[`.
- Enter `chatty_logs` → drill into `chat` → `/error` `Enter` `n` — jumps
  through highlighted ERROR occurrences.
