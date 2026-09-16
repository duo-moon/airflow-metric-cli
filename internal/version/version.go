// Package version holds build-time metadata populated via -ldflags.
package version

// Version is the released version (e.g. "v0.3.1" or "dev").
var Version = "dev"

// Commit is the short git SHA at build time.
var Commit = "none"

// Date is the RFC3339 timestamp of the build.
var Date = "unknown"
