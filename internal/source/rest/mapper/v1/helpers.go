package v1

import "time"

func deref(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func derefInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// parseRFC3339 parses a *string time field. Silently returns zero time on
// nil / empty / malformed input — callers must not treat "no data" and
// "bad data" differently. Airflow occasionally sends non-RFC3339 dates on
// legacy endpoints, so a hard error here would break the UI on live data.
func parseRFC3339(s *string) time.Time {
	if s == nil || *s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, *s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func latest(ts ...time.Time) time.Time {
	var out time.Time
	for _, t := range ts {
		if t.After(out) {
			out = t
		}
	}
	return out
}
