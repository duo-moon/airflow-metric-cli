# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2026-09-16

First public release.

### Features

- **TUI dashboard** built on [Bubble Tea](https://github.com/charmbracelet/bubbletea):
  cluster health, Active Runs (with `WAIT` counter for queued/deferred/retry tasks),
  Recent Failures, Pools with utilisation bars, import-errors badge.
- **Drill-down** — `enter` on a DAG run to see its task instances; `enter` on
  a task instance to open its logs.
- **Log viewer** with per-attempt navigation (`[` / `]`), search (`/`, `n`, `N`),
  scroll (`↑↓/jk`, `g/G`), level-aware colorization (INFO/WARN/ERROR).
- **Two REST API dialects** behind a single `AirflowClient` interface:
  `--api-version v1` for Airflow 2.x (Connexion, `/api/v1`) and `--api-version v2`
  for Airflow 3.x (FastAPI, `/api/v2`, JWT-only). Pinned specs live in
  `api/VERSION` (currently apache/airflow 2.11.2 and 3.2.0).
- **Adaptive rate-limit Guard**: doubles a task's polling interval when its error
  rate exceeds a threshold, halves it back on recovery.
- **Zero-config**: all settings via env vars (`AIRFLOW_URL`, `AIRFLOW_TOKEN`,
  `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD`) and CLI flags. Multi-cluster setups use
  shell aliases.
- **`ping`** command for one-shot connectivity + component-health check
  (CI-friendly exit codes).
- **Local demo stand** (`demo/docker-compose.yaml`) behind docker-compose
  profiles for both dialects: `v2` boots apache/airflow 2.11.2 + `/api/v1`,
  `v3` boots apache/airflow 3.2.0 + `/api/v2` + JWT. Wrapped by Makefile
  targets `demo-up[-v3]`, `demo-run[-v3]`, `demo-down[-v3]`, and
  `demo-token-v3` (fetches a JWT via `/auth/token`). Ships 11 sample DAGs
  exercising every panel: healthy pipeline, flaky retries, multi-try
  navigation, always-fails, long-running, chatty logs, sensor wait,
  deferred wait, pool contention, skip chains, broken DAG file for import
  errors.

### Distribution

- Static `afmetric` binary (`goreleaser`) for `linux/darwin × amd64/arm64`.
- Distroless-based Docker image (~5 MB) via `make docker-build`.

[Unreleased]: https://github.com/duo-moon/airflow-metric-cli/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/duo-moon/airflow-metric-cli/releases/tag/v0.1.0
