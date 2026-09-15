package model

// Pool is a slot pool that gates how many tasks can run concurrently.
type Pool struct {
	Name          string
	Slots         int
	OccupiedSlots int
	RunningSlots  int
	QueuedSlots   int
	OpenSlots     int
	DeferredSlots int
	Description   string
}

// UsedSlots is the effective count of "held" slots — running/queued plus
// deferred tasks (which reserve their slot when the pool was configured
// with include_deferred=true). This matches Airflow's own definition of
// occupancy on include-deferred pools.
func (p Pool) UsedSlots() int {
	return p.OccupiedSlots + p.DeferredSlots
}

// Utilization returns the fraction of used slots in [0, 1]. Zero-sized
// pools report 0.
func (p Pool) Utilization() float64 {
	if p.Slots <= 0 {
		return 0
	}
	u := float64(p.UsedSlots()) / float64(p.Slots)
	if u > 1 {
		u = 1
	}
	return u
}
