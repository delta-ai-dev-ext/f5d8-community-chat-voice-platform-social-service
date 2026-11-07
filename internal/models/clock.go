package models

import "time"

// Clock supplies the current time. It is injected everywhere a timestamp is
// produced so tests are deterministic.
type Clock interface {
	Now() time.Time
}

// RealClock is the production clock; it always returns UTC.
type RealClock struct{}

// Now returns the current time in UTC.
func (RealClock) Now() time.Time { return time.Now().UTC() }
