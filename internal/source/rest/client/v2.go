package client

// This file adapts the REST v2 generated client. The `mapper` import alias
// resolves to internal/source/rest/mapper/v2 — v1.go uses the same alias
// against mapper/v1, keeping both adapters visually symmetric.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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
		if text := decodeV2LogContent(&resp.JSON200.Content); text != "" {
			return text, nil
		}
	}
	if len(resp.Body) > 0 {
		return string(resp.Body), nil
	}
	return "", fmt.Errorf("empty log response")
}

// decodeV2LogContent flattens the v2 log content union into a single string.
// The REST v2 log endpoint returns either a list of structured messages
// ({event, timestamp, ...}) or a plain []string; we join both variants with
// newlines.
func decodeV2LogContent(u *airflowv2.TaskInstancesLogResponse_Content) string {
	// Try structured messages first (the default JSON representation).
	if structured, err := u.AsTaskInstancesLogResponseContent0(); err == nil && len(structured) > 0 {
		var b strings.Builder
		for i, m := range structured {
			if i > 0 {
				b.WriteByte('\n')
			}
			if m.Timestamp != nil {
				b.WriteString(m.Timestamp.Format("2006-01-02T15:04:05Z"))
				b.WriteByte(' ')
			}
			b.WriteString(m.Event)
			for k, v := range m.AdditionalProperties {
				if raw, err := json.Marshal(v); err == nil {
					b.WriteString(" ")
					b.WriteString(k)
					b.WriteByte('=')
					b.Write(raw)
				}
			}
		}
		return b.String()
	}
	// Fall back to a plain string list.
	if lines, err := u.AsTaskInstancesLogResponseContent1(); err == nil && len(lines) > 0 {
		return strings.Join(lines, "\n")
	}
	return ""
}
