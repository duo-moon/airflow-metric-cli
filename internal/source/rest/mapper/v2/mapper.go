// Package v2 maps Airflow REST API v2 DTOs (from the generated `airflowv2`
// client) into the internal domain model. Every function is pure: no
// I/O, no logging, no time.Now() — pass a clock in when needed.
//
// Unlike v1, v2 schemas mark most identifiers as non-nullable, so single-
// item mappers can safely return value types. Batch wrappers keep every
// row — there's nothing to skip.
package v2

import (
	"slices"
	"strings"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/airflowv2"
)

// ---------- health ----------

// ClusterHealthNow stamps ClusterHealth with time.Now().
func ClusterHealthNow(h *airflowv2.HealthInfoResponse) model.ClusterHealth {
	return ClusterHealth(h, time.Now())
}

// ClusterHealth converts /monitor/health with the caller's clock.
//
// v2 schema declares scheduler / metadatabase / triggerer as non-pointer
// sub-objects, so we don't guard against nil for them. DagProcessor is
// still optional and gets a nil check.
func ClusterHealth(h *airflowv2.HealthInfoResponse, observedAt time.Time) model.ClusterHealth {
	if h == nil {
		return model.ClusterHealth{ObservedAt: observedAt}
	}
	out := model.ClusterHealth{ObservedAt: observedAt}
	out.Components = append(out.Components, model.ComponentHealth{
		Name:            "Scheduler",
		Status:          mapHealthStatus(h.Scheduler.Status),
		LatestHeartbeat: parseRFC3339(h.Scheduler.LatestSchedulerHeartbeat),
	})
	out.Components = append(out.Components, model.ComponentHealth{
		Name:   "Database",
		Status: mapHealthStatus(h.Metadatabase.Status),
	})
	out.Components = append(out.Components, model.ComponentHealth{
		Name:            "Triggerer",
		Status:          mapHealthStatus(h.Triggerer.Status),
		LatestHeartbeat: parseRFC3339(h.Triggerer.LatestTriggererHeartbeat),
	})
	if h.DagProcessor != nil {
		out.Components = append(out.Components, model.ComponentHealth{
			Name:            "DagProcessor",
			Status:          mapHealthStatus(h.DagProcessor.Status),
			LatestHeartbeat: parseRFC3339(h.DagProcessor.LatestDagProcessorHeartbeat),
		})
	}
	return out
}

func mapHealthStatus(s *string) model.HealthStatus {
	if s == nil {
		return model.HealthUnknown
	}
	switch strings.ToLower(*s) {
	case "healthy":
		return model.HealthHealthy
	case "unhealthy":
		return model.HealthUnhealthy
	default:
		return model.HealthUnknown
	}
}

// ---------- dag runs ----------

// DagRun maps one DAGRunResponse. v2 guarantees dag_id and dag_run_id are
// non-null, so this always returns a populated value (nil-in yields zero).
func DagRun(r *airflowv2.DAGRunResponse) model.DagRun {
	if r == nil {
		return model.DagRun{}
	}
	out := model.DagRun{
		DagID:       r.DagId,
		RunID:       r.DagRunId,
		State:       model.DagRunState(r.State),
		RunType:     string(r.RunType),
		LogicalDate: deref(r.LogicalDate),
		Start:       deref(r.StartDate),
		End:         deref(r.EndDate),
		Note:        derefString(r.Note),
	}
	out.UpdatedAt = latest(out.End, out.Start, out.LogicalDate)
	return out
}

// DagRuns maps a slice.
func DagRuns(in []airflowv2.DAGRunResponse) []model.DagRun {
	out := make([]model.DagRun, 0, len(in))
	for i := range in {
		out = append(out, DagRun(&in[i]))
	}
	return out
}

// ---------- pools ----------

// Pool maps one PoolResponse.
func Pool(p *airflowv2.PoolResponse) model.Pool {
	if p == nil {
		return model.Pool{}
	}
	return model.Pool{
		Name:          p.Name,
		Slots:         p.Slots,
		OccupiedSlots: p.OccupiedSlots,
		RunningSlots:  p.RunningSlots,
		QueuedSlots:   p.QueuedSlots,
		OpenSlots:     p.OpenSlots,
		DeferredSlots: p.DeferredSlots,
		Description:   derefString(p.Description),
	}
}

// Pools maps a slice.
func Pools(in []airflowv2.PoolResponse) []model.Pool {
	out := make([]model.Pool, 0, len(in))
	for i := range in {
		out = append(out, Pool(&in[i]))
	}
	return out
}

// ---------- import errors ----------

// ImportError maps one ImportErrorResponse.
func ImportError(e *airflowv2.ImportErrorResponse) model.ImportError {
	if e == nil {
		return model.ImportError{}
	}
	return model.ImportError{
		ID:        e.ImportErrorId,
		Filename:  e.Filename,
		Trace:     e.StackTrace,
		Timestamp: e.Timestamp,
	}
}

// ImportErrors maps a slice.
func ImportErrors(in []airflowv2.ImportErrorResponse) []model.ImportError {
	out := make([]model.ImportError, 0, len(in))
	for i := range in {
		out = append(out, ImportError(&in[i]))
	}
	return out
}

// ---------- task instances ----------

// TaskInstance maps one TaskInstanceResponse.
func TaskInstance(t *airflowv2.TaskInstanceResponse) model.TaskInstance {
	if t == nil {
		return model.TaskInstance{}
	}
	out := model.TaskInstance{
		DagID:     t.DagId,
		RunID:     t.DagRunId,
		TaskID:    t.TaskId,
		TryNumber: t.TryNumber,
		MaxTries:  t.MaxTries,
		MapIndex:  t.MapIndex,
		Operator:  derefString(t.Operator),
		Executor:  derefString(t.Executor),
		Pool:      t.Pool,
		Queue:     derefString(t.Queue),
		Hostname:  derefString(t.Hostname),
		Start:     deref(t.StartDate),
		End:       deref(t.EndDate),
	}
	if t.State != nil {
		out.State = model.TaskState(*t.State)
	}
	if t.Duration != nil {
		out.Duration = time.Duration(float64(*t.Duration) * float64(time.Second))
	} else if !out.Start.IsZero() && !out.End.IsZero() {
		out.Duration = out.End.Sub(out.Start)
	}
	return out
}

// TaskInstances maps a slice.
func TaskInstances(in []airflowv2.TaskInstanceResponse) []model.TaskInstance {
	out := make([]model.TaskInstance, 0, len(in))
	for i := range in {
		out = append(out, TaskInstance(&in[i]))
	}
	return out
}

// ---------- task tries ----------

// TaskAttempts maps the /tries collection into ordered model attempts.
func TaskAttempts(in []airflowv2.TaskInstanceHistoryResponse) []model.TaskAttempt {
	out := make([]model.TaskAttempt, 0, len(in))
	for i := range in {
		h := &in[i]
		a := model.TaskAttempt{
			TryNumber: h.TryNumber,
			Start:     deref(h.StartDate),
			End:       deref(h.EndDate),
		}
		if h.State != nil {
			a.State = model.TaskState(*h.State)
		}
		out = append(out, a)
	}
	slices.SortFunc(out, func(a, b model.TaskAttempt) int {
		return a.TryNumber - b.TryNumber
	})
	return out
}

// ---------- waiting-tasks aggregation ----------

// AggregateWaiting groups v2 task instances by (dag_id, run_id) and counts
// them per waiting-state. See v1.AggregateWaiting — same logic, different
// input schema (non-pointer ids in v2).
func AggregateWaiting(items []airflowv2.TaskInstanceResponse) []model.WaitingCount {
	type key struct{ dag, run string }
	agg := make(map[key]*model.WaitingCount)
	for i := range items {
		it := &items[i]
		if it.State == nil {
			continue
		}
		k := key{it.DagId, it.DagRunId}
		w, ok := agg[k]
		if !ok {
			w = &model.WaitingCount{DagID: k.dag, RunID: k.run}
			agg[k] = w
		}
		switch model.TaskState(*it.State) {
		case model.TaskUpForReschedule:
			w.Reschedule++
		case model.TaskUpForRetry:
			w.Retry++
		case model.TaskScheduled:
			w.Scheduled++
		case model.TaskQueued:
			w.Queued++
		case model.TaskDeferred:
			w.Deferred++
		}
	}
	out := make([]model.WaitingCount, 0, len(agg))
	for _, w := range agg {
		out = append(out, *w)
	}
	return out
}
