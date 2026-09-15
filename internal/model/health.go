package model

import "time"

// HealthStatus captures the state of a single cluster component.
type HealthStatus string

// Possible values for HealthStatus.
const (
	HealthHealthy   HealthStatus = "healthy"
	HealthUnhealthy HealthStatus = "unhealthy"
	HealthUnknown   HealthStatus = "unknown"
)

// ComponentHealth describes a single Airflow subsystem — scheduler,
// metadatabase, triggerer or DAG processor.
type ComponentHealth struct {
	Name            string
	Status          HealthStatus
	LatestHeartbeat time.Time // zero if not reported
}

// ClusterHealth is a snapshot of the cluster's component states at
// ObservedAt.
type ClusterHealth struct {
	Components []ComponentHealth
	ObservedAt time.Time
}

// AllHealthy reports whether every known component is HealthHealthy.
func (c ClusterHealth) AllHealthy() bool {
	if len(c.Components) == 0 {
		return false
	}
	return len(c.Unhealthy()) == 0
}

// Unhealthy returns components that are neither HealthHealthy nor
// HealthUnknown — i.e. the ones you'd want to page an operator about.
// Unknown is excluded on purpose: it means "the API didn't tell us", not
// "broken".
func (c ClusterHealth) Unhealthy() []ComponentHealth {
	var out []ComponentHealth
	for _, comp := range c.Components {
		if comp.Status != HealthHealthy && comp.Status != HealthUnknown {
			out = append(out, comp)
		}
	}
	return out
}
