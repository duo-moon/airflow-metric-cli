// Package v1 maps Airflow REST API v1 DTOs (from the generated `airflow`
// client) into the internal domain model. Every function is pure: no
// I/O, no logging, no time.Now() — pass a clock in when needed.
//
// Nil-return convention: single-item mappers return *T so a missing
// required key (dag_id, run_id, task_id, error id, pool name) yields nil.
// Batch wrappers skip such entries silently.
package v1

import (
	"slices"
	"time"

	"github.com/duo-moon/airflow-metric-cli/internal/model"
	"github.com/duo-moon/airflow-metric-cli/internal/source/rest/airflowv1"
)

// ---------- health ----------

// ClusterHealthNow stamps ClusterHealth with time.Now().
func ClusterHealthNow(h *airflowv1.HealthInfo) model.ClusterHealth {
	return ClusterHealth(h, time.Now())
}

// ClusterHealth converts /health with an observation timestamp supplied by
// the caller (so tests can freeze time).
func ClusterHealth(h *airflowv1.HealthInfo, observedAt time.Time) model.ClusterHealth {
	if h == nil {
		return model.ClusterHealth{ObservedAt: observedAt}
	}
	out := model.ClusterHealth{ObservedAt: observedAt}
	if h.Scheduler != nil {
		out.Components = append(out.Components, model.ComponentHealth{
			Name:            "Scheduler",
			Status:          mapHealthStatus(h.Scheduler.Status),
			LatestHeartbeat: parseRFC3339(h.Scheduler.LatestSchedulerHeartbeat),
		})
	}
	if h.Metadatabase != nil {
		out.Components = append(out.Components, model.ComponentHealth{
			Name:   "Database",
			Status: mapHealthStatus(h.Metadatabase.Status),
		})
	}
	if h.Triggerer != nil {
		out.Components = append(out.Components, model.ComponentHealth{
			Name:            "Triggerer",
			Status:          mapHealthStatus(h.Triggerer.Status),
			LatestHeartbeat: parseRFC3339(h.Triggerer.LatestTriggererHeartbeat),
		})
	}
	if h.DagProcessor != nil {
		out.Components = append(out.Components, model.ComponentHealth{
			Name:            "DagProcessor",
			Status:          mapHealthStatus(h.DagProcessor.Status),
			LatestHeartbeat: parseRFC3339(h.DagProcessor.LatestDagProcessorHeartbeat),
		})
	}
	return out
}

func mapHealthStatus(s *airflowv1.HealthStatus) model.HealthStatus {
	if s == nil {
		return model.HealthUnknown
	}
	switch string(*s) {
	case "healthy":
		return model.HealthHealthy
	case "unhealthy":
		return model.HealthUnhealthy
	default:
		return model.HealthUnknown
	}
}

// ---------- dag runs ----------

// DagRun maps one DAGRun. Returns nil if dag_id or dag_run_id are missing.
func DagRun(r *airflowv1.DAGRun) *model.DagRun {
	if r == nil || r.DagId == nil || r.DagRunId == nil {
		return nil
	}
	out := &model.DagRun{
		DagID:       *r.DagId,
		RunID:       *r.DagRunId,
		State:       mapDagRunState(r.State),
		LogicalDate: deref(r.LogicalDate),
		Start:       deref(r.StartDate),
		End:         deref(r.EndDate),
		Note:        derefString(r.Note),
	}
	if r.RunType != nil {
		out.RunType = string(*r.RunType)
	}
	// v1 API omits updated_at — approximate with the latest known timestamp
	// so the store's freshness watermark still moves forward.
	out.UpdatedAt = latest(out.End, out.Start, out.LogicalDate)
	return out
}

// DagRuns maps a collection, skipping entries missing required keys.
func DagRuns(in []airflowv1.DAGRun) []model.DagRun {
	out := make([]model.DagRun, 0, len(in))
	for i := range in {
		if m := DagRun(&in[i]); m != nil {
			out = append(out, *m)
		}
	}
	return out
}

func mapDagRunState(s *airflowv1.DagState) model.DagRunState {
	if s == nil {
		return ""
	}
	return model.DagRunState(*s)
}

// ---------- pools ----------

// Pool maps one pool. Returns nil if name is missing.
func Pool(p *airflowv1.Pool) *model.Pool {
	if p == nil || p.Name == nil {
		return nil
	}
	return &model.Pool{
		Name:          *p.Name,
		Slots:         derefInt(p.Slots),
		OccupiedSlots: derefInt(p.OccupiedSlots),
		RunningSlots:  derefInt(p.RunningSlots),
		QueuedSlots:   derefInt(p.QueuedSlots),
		OpenSlots:     derefInt(p.OpenSlots),
		DeferredSlots: derefInt(p.DeferredSlots),
		Description:   derefString(p.Description),
	}
}

// Pools maps a collection.
func Pools(in []airflowv1.Pool) []model.Pool {
	out := make([]model.Pool, 0, len(in))
	for i := range in {
		if m := Pool(&in[i]); m != nil {
			out = append(out, *m)
		}
	}
	return out
}

// ---------- import errors ----------

// ImportError maps a single import error. Returns nil if ID is missing.
func ImportError(e *airflowv1.ImportError) *model.ImportError {
	if e == nil || e.ImportErrorId == nil {
		return nil
	}
	return &model.ImportError{
		ID:        *e.ImportErrorId,
		Filename:  derefString(e.Filename),
		Trace:     derefString(e.StackTrace),
		Timestamp: parseRFC3339(e.Timestamp),
	}
}

// ImportErrors maps a collection.
func ImportErrors(in []airflowv1.ImportError) []model.ImportError {
	out := make([]model.ImportError, 0, len(in))
	for i := range in {
		if m := ImportError(&in[i]); m != nil {
			out = append(out, *m)
		}
	}
	return out
}

// ---------- task instances ----------

// TaskInstance maps one task instance. Returns nil if task_id or dag_id are
// missing.
func TaskInstance(t *airflowv1.TaskInstance) *model.TaskInstance {
	if t == nil || t.DagId == nil || t.TaskId == nil {
		return nil
	}
	out := &model.TaskInstance{
		DagID:     *t.DagId,
		RunID:     derefString(t.DagRunId),
		TaskID:    *t.TaskId,
		TryNumber: derefInt(t.TryNumber),
		MaxTries:  derefInt(t.MaxTries),
		MapIndex:  derefInt(t.MapIndex),
		Operator:  derefString(t.Operator),
		Executor:  derefString(t.Executor),
		Pool:      derefString(t.Pool),
		Queue:     derefString(t.Queue),
		Hostname:  derefString(t.Hostname),
		Start:     parseRFC3339(t.StartDate),
		End:       parseRFC3339(t.EndDate),
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

// TaskInstances maps a collection, skipping entries missing required keys.
func TaskInstances(in []airflowv1.TaskInstance) []model.TaskInstance {
	out := make([]model.TaskInstance, 0, len(in))
	for i := range in {
		if m := TaskInstance(&in[i]); m != nil {
			out = append(out, *m)
		}
	}
	return out
}

// ---------- task tries ----------

// TaskAttempts maps the /tries collection into ordered model attempts.
// Entries without try_number are skipped; result is sorted ascending.
func TaskAttempts(in []airflowv1.TaskInstanceHistory) []model.TaskAttempt {
	out := make([]model.TaskAttempt, 0, len(in))
	for i := range in {
		h := &in[i]
		if h.TryNumber == nil {
			continue
		}
		a := model.TaskAttempt{
			TryNumber: *h.TryNumber,
			Start:     parseRFC3339(h.StartDate),
			End:       parseRFC3339(h.EndDate),
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

// AggregateWaiting groups task instances by (dag_id, run_id) and counts them
// per waiting-state. Rows missing ids or state are skipped so a mis-shaped
// item doesn't crash the panel.
//
// Sits in mapper rather than in the collector because it operates on the
// generated DTO directly — the collector never sees raw airflowv1.* types.
func AggregateWaiting(items []airflowv1.TaskInstance) []model.WaitingCount {
	type key struct{ dag, run string }
	agg := make(map[key]*model.WaitingCount)
	for i := range items {
		it := &items[i]
		if it.DagId == nil || it.DagRunId == nil || it.State == nil {
			continue
		}
		k := key{*it.DagId, *it.DagRunId}
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
