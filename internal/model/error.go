package model

import "time"

// ImportError is a DAG-file parse failure.
type ImportError struct {
	ID        int
	Filename  string
	Trace     string
	Timestamp time.Time
}
