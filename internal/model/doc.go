// Package model contains the domain types afmetric works with internally.
//
// Types here are plain value objects: no accessors that hit I/O, no
// knowledge of how or where they get populated. Any code that produces or
// consumes them (data sources, storage, UI) depends on this package, not
// the other way around.
package model
