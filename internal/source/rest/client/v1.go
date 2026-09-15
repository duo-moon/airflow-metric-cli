package client

// This file adapts the REST v1 generated client. The `mapper` import alias
// resolves to internal/source/rest/mapper/v1 — v2.go uses the same alias
// against mapper/v2, keeping both adapters visually symmetric.

import (
	"context"
	"fmt"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/airflowv1"
	mapper "github.com/duo-moon/airflow-metric-cli/internal/source/rest/mapper/v1"
)

// v1Adapter implements AirflowClient against the REST API v1 generated client.
type v1Adapter struct {
	client *airflowv1.ClientWithResponses
}

func newV1Adapter(c *airflowv1.ClientWithResponses) AirflowClient {
	return &v1Adapter{client: c}
}

func (a *v1Adapter) Version(ctx context.Context) (string, string, error) {
	resp, err := a.client.GetVersionWithResponse(ctx)
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return "", "", fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	var v, git string
	if resp.JSON200.Version != nil {
		v = *resp.JSON200.Version
	}
	if resp.JSON200.GitVersion != nil {
		git = *resp.JSON200.GitVersion
	}
	return v, git, nil
}

func (a *v1Adapter) Health(ctx context.Context) (model.ClusterHealth, error) {
	resp, err := a.client.GetHealthWithResponse(ctx)
	if err != nil {
		return model.ClusterHealth{}, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		return model.ClusterHealth{}, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.ClusterHealthNow(resp.JSON200), nil
}

func (a *v1Adapter) DagRuns(ctx context.Context, states []model.DagRunState, pageLimit int) ([]model.DagRun, error) {
	rawStates := make([]string, 0, len(states))
	for _, s := range states {
		rawStates = append(rawStates, string(s))
	}
	orderBy := "-start_date"
	body := airflowv1.GetDagRunsBatchJSONRequestBody{
		States:    &rawStates,
		OrderBy:   &orderBy,
		PageLimit: &pageLimit,
	}
	resp, err := a.client.GetDagRunsBatchWithResponse(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil || resp.JSON200.DagRuns == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.DagRuns(*resp.JSON200.DagRuns), nil
}

func (a *v1Adapter) Pools(ctx context.Context) ([]model.Pool, error) {
	resp, err := a.client.GetPoolsWithResponse(ctx, &airflowv1.GetPoolsParams{})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil || resp.JSON200.Pools == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.Pools(*resp.JSON200.Pools), nil
}

func (a *v1Adapter) ImportErrors(ctx context.Context) ([]model.ImportError, error) {
	resp, err := a.client.GetImportErrorsWithResponse(ctx, &airflowv1.GetImportErrorsParams{})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil || resp.JSON200.ImportErrors == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.ImportErrors(*resp.JSON200.ImportErrors), nil
}

func (a *v1Adapter) WaitingTasks(ctx context.Context, pageLimit int) ([]model.WaitingCount, error) {
	rescheduleState := airflowv1.TaskState("up_for_reschedule")
	retryState := airflowv1.TaskState("up_for_retry")
	scheduledState := airflowv1.TaskState("scheduled")
	queuedState := airflowv1.TaskState("queued")
	deferredState := airflowv1.TaskState("deferred")
	states := []*airflowv1.TaskState{&rescheduleState, &retryState, &scheduledState, &queuedState, &deferredState}
	body := airflowv1.GetTaskInstancesBatchJSONRequestBody{
		State:     &states,
		PageLimit: &pageLimit,
	}
	resp, err := a.client.GetTaskInstancesBatchWithResponse(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil || resp.JSON200.TaskInstances == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.AggregateWaiting(*resp.JSON200.TaskInstances), nil
}

func (a *v1Adapter) TaskInstances(ctx context.Context, dagID, runID string) ([]model.TaskInstance, error) {
	resp, err := a.client.GetTaskInstancesWithResponse(ctx, dagID, runID, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil || resp.JSON200.TaskInstances == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.TaskInstances(*resp.JSON200.TaskInstances), nil
}

func (a *v1Adapter) TaskTries(ctx context.Context, dagID, runID, taskID string) ([]model.TaskAttempt, error) {
	resp, err := a.client.GetTaskInstanceTriesWithResponse(ctx, dagID, runID, taskID, &airflowv1.GetTaskInstanceTriesParams{})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil || resp.JSON200.TaskInstancesHistory == nil {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	return mapper.TaskAttempts(*resp.JSON200.TaskInstancesHistory), nil
}

func (a *v1Adapter) TaskLogs(ctx context.Context, dagID, runID, taskID string, tryNumber int) (string, error) {
	if tryNumber < 1 {
		tryNumber = 1
	}
	full := true
	resp, err := a.client.GetLogWithResponse(ctx, dagID, runID, taskID, tryNumber, &airflowv1.GetLogParams{FullContent: &full})
	if err != nil {
		return "", err
	}
	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	if resp.JSON200 != nil && resp.JSON200.Content != nil {
		return *resp.JSON200.Content, nil
	}
	if len(resp.Body) > 0 {
		return string(resp.Body), nil
	}
	return "", fmt.Errorf("empty log response")
}
