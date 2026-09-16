package client

// This file adapts the REST v2 generated client. The `mapper` import alias
// resolves to internal/source/rest/mapper/v2 — v1.go uses the same alias
// against mapper/v1, keeping both adapters visually symmetric.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/airflowv2"
	mapper "github.com/duo-moon/airflow-metric-cli/internal/source/rest/mapper/v2"
)

// v2Adapter implements AirflowClient against the REST API v2 generated client.
type v2Adapter struct {
	client *airflowv2.ClientWithResponses
}

func newV2Adapter(c *airflowv2.ClientWithResponses) AirflowClient {
	return &v2Adapter{client: c}
}

func (a *v2Adapter) Version(ctx context.Context) (string, string, error) {
	resp, err := a.client.GetVersionWithResponse(ctx)
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return "", "", fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	var git string
	if resp.JSON200.GitVersion != nil {
		git = *resp.JSON200.GitVersion
	}
	return resp.JSON200.Version, git, nil
}

func (a *v2Adapter) Health(ctx context.Context) (model.ClusterHealth, error) {
	resp, err := a.client.GetHealthWithResponse(ctx)
	if err != nil {
		return model.ClusterHealth{}, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return model.ClusterHealth{}, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.ClusterHealthNow(resp.JSON200), nil
}

func (a *v2Adapter) DagRuns(ctx context.Context, states []model.DagRunState, pageLimit int) ([]model.DagRun, error) {
	// v2 wants []*DagRunState — build the pointer slice.
	rawStates := make([]*airflowv2.DagRunState, 0, len(states))
	for _, s := range states {
		v := airflowv2.DagRunState(s)
		rawStates = append(rawStates, &v)
	}
	orderBy := "-start_date"
	body := airflowv2.GetListDagRunsBatchJSONRequestBody{
		States:    &rawStates,
		OrderBy:   &orderBy,
		PageLimit: &pageLimit,
	}
	resp, err := a.client.GetListDagRunsBatchWithResponse(ctx, airflowv2.GetListDagRunsBatchParamsDagIdTilde, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.DagRuns(resp.JSON200.DagRuns), nil
}

func (a *v2Adapter) Pools(ctx context.Context) ([]model.Pool, error) {
	resp, err := a.client.GetPoolsWithResponse(ctx, &airflowv2.GetPoolsParams{})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.Pools(resp.JSON200.Pools), nil
}

func (a *v2Adapter) ImportErrors(ctx context.Context) ([]model.ImportError, error) {
	resp, err := a.client.GetImportErrorsWithResponse(ctx, &airflowv2.GetImportErrorsParams{})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.ImportErrors(resp.JSON200.ImportErrors), nil
}

func (a *v2Adapter) WaitingTasks(ctx context.Context, pageLimit int) ([]model.WaitingCount, error) {
	rescheduleState := airflowv2.TaskInstanceState("up_for_reschedule")
	retryState := airflowv2.TaskInstanceState("up_for_retry")
	scheduledState := airflowv2.TaskInstanceState("scheduled")
	queuedState := airflowv2.TaskInstanceState("queued")
	deferredState := airflowv2.TaskInstanceState("deferred")
	states := []*airflowv2.TaskInstanceState{&rescheduleState, &retryState, &scheduledState, &queuedState, &deferredState}
	body := airflowv2.GetTaskInstancesBatchJSONRequestBody{
		State:     &states,
		PageLimit: &pageLimit,
	}
	resp, err := a.client.GetTaskInstancesBatchWithResponse(ctx,
		airflowv2.GetTaskInstancesBatchParamsDagIdTilde,
		airflowv2.GetTaskInstancesBatchParamsDagRunIdTilde,
		body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.AggregateWaiting(resp.JSON200.TaskInstances), nil
}

func (a *v2Adapter) TaskInstances(ctx context.Context, dagID, runID string) ([]model.TaskInstance, error) {
	resp, err := a.client.GetTaskInstancesWithResponse(ctx, dagID, runID, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.TaskInstances(resp.JSON200.TaskInstances), nil
}

func (a *v2Adapter) TaskTries(ctx context.Context, dagID, runID, taskID string) ([]model.TaskAttempt, error) {
	resp, err := a.client.GetTaskInstanceTriesWithResponse(ctx, dagID, runID, taskID, &airflowv2.GetTaskInstanceTriesParams{})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.TaskAttempts(resp.JSON200.TaskInstances), nil
}

func (a *v2Adapter) TaskLogs(ctx context.Context, dagID, runID, taskID string, tryNumber int) (string, error) {
	if tryNumber < 1 {
		tryNumber = 1
	}
	full := true
	resp, err := a.client.GetLogWithResponse(ctx, dagID, runID, taskID, tryNumber, &airflowv2.GetLogParams{FullContent: &full})
	if err != nil {
		return "", err
	}
	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	if resp.JSON200 != nil {
		// Empty union → empty string (mirrors v1 behaviour). Never leak the
		// raw response body here — it's JSON and would be shown verbatim in
		// the log viewer, which is worse than an "(empty log)" placeholder.
		return decodeV2LogContent(&resp.JSON200.Content), nil
	}
	return "", fmt.Errorf("empty log response")
}

// decodeV2LogContent flattens the v2 log content union into a single string.
// The REST v2 log endpoint returns either a list of structured messages
// ({event, timestamp, level, ...}) or a plain []string; we join both
// variants with newlines.
//
// Structured entries are rendered in the classic Airflow 2.x layout
//
//	TIMESTAMP LEVEL - message key=val ...
//
// so the UI's level-colorizer (which matches `(^|\s)LEVEL - `) picks them
// up without a v3-specific code path. `level` (and `log_level` when it
// stood in as the level source) are consumed into the header; remaining
// structlog keys ride along as `key=val` tail.
func decodeV2LogContent(u *airflowv2.TaskInstancesLogResponse_Content) string {
	if structured, err := u.AsTaskInstancesLogResponseContent0(); err == nil && len(structured) > 0 {
		var b strings.Builder
		for i := range structured {
			if i > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(formatStructuredLogMessage(&structured[i]))
		}
		return b.String()
	}
	if lines, err := u.AsTaskInstancesLogResponseContent1(); err == nil && len(lines) > 0 {
		return strings.Join(lines, "\n")
	}
	return ""
}

// formatStructuredLogMessage renders one structlog entry as
// `TIMESTAMP LEVEL - event key=val ...`. Level is always emitted (defaulting
// to INFO) so the colorizer regex has an anchor even when the source entry
// omits severity. Timestamps are normalised to UTC RFC3339; without one the
// line starts with `LEVEL - ` and the colorizer's `^`-alternate matches.
//
// String values in extras are rendered bare (mirrors Airflow's classic
// `key=value` style, so grep patterns like `task_id=extract` transfer);
// non-strings are JSON-encoded. Extras keys are sorted alphabetically —
// snapshot-test determinism trumps the loss of structlog's binding order,
// which the batched JSON response often doesn't preserve anyway.
func formatStructuredLogMessage(m *airflowv2.StructuredLogMessage) string {
	var b strings.Builder
	if m.Timestamp != nil {
		b.WriteString(m.Timestamp.UTC().Format(time.RFC3339))
		b.WriteByte(' ')
	}
	level, levelKey := structlogLevel(m.AdditionalProperties)
	b.WriteString(level)
	b.WriteString(" - ")
	b.WriteString(m.Event)

	extras := make([]string, 0, len(m.AdditionalProperties))
	for k := range m.AdditionalProperties {
		if k == levelKey {
			continue
		}
		extras = append(extras, k)
	}
	sort.Strings(extras)
	for _, k := range extras {
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(formatStructlogValue(m.AdditionalProperties[k]))
	}
	return b.String()
}

// structlogLevel returns the canonical uppercase level string extracted from
// a structlog entry, plus the key it was drawn from ("" when no key was
// consumed and the INFO fallback is used).
//
// Prefers `level` over `log_level`. If the picked key holds a string, it's
// uppercased. If it holds a Python-logging numeric (int/float from JSON),
// it's mapped by threshold (10=DEBUG..50=CRITICAL) so a value like 40 shows
// as ERROR rather than being silently downgraded to INFO. Unrecognised
// types fall through to the fallback and — importantly — do NOT consume the
// key, so the raw value still surfaces in the extras tail.
func structlogLevel(props map[string]interface{}) (level, key string) {
	for _, k := range []string{"level", "log_level"} {
		raw, ok := props[k]
		if !ok {
			continue
		}
		if lvl, ok := coerceStructlogLevel(raw); ok {
			return lvl, k
		}
	}
	return "INFO", ""
}

func coerceStructlogLevel(raw interface{}) (string, bool) {
	switch v := raw.(type) {
	case string:
		if v == "" {
			return "", false
		}
		return strings.ToUpper(v), true
	case float64:
		return numericLevel(int(v)), true
	case int:
		return numericLevel(v), true
	}
	return "", false
}

// numericLevel maps Python logging integer levels to Airflow's textual
// severities using the same thresholds as the stdlib logger (DEBUG=10,
// INFO=20, WARNING=30, ERROR=40, CRITICAL=50). Values in between land on
// the next lower named level, matching what stdlib does when formatting.
func numericLevel(n int) string {
	switch {
	case n >= 50:
		return "CRITICAL"
	case n >= 40:
		return "ERROR"
	case n >= 30:
		return "WARNING"
	case n >= 20:
		return "INFO"
	default:
		return "DEBUG"
	}
}

// formatStructlogValue renders one extras value: bare for strings (so
// `task_id=extract` grep patterns work regardless of dialect), JSON for
// everything else. Errors from json.Marshal collapse to an empty string —
// callers already emitted the `key=` prefix, so worst case is `key=`.
func formatStructlogValue(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(raw)
}
