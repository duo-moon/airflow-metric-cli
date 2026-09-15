package v2

import "time"

// deref returns *t or zero on nil.
func deref(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// derefString returns *s or "" on nil.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// parseRFC3339 mirrors the v1 helper for the odd v2 field that still ships
// as *string (heartbeats). Silent-fails to zero time.
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

// latest returns the max non-zero time from the arguments.
func latest(ts ...time.Time) time.Time {
	var out time.Time
	for _, t := range ts {
		if t.After(out) {
			out = t
		}
	}
	return out
}
