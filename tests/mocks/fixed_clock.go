// Package mocks holds deterministic test doubles: a fixed clock, an in-process
// Kafka broker fake, and a stubbed token verifier. No test talks to PostgreSQL,
// Kafka, or the network.
package mocks

import (
	"sync"
	"time"

	"social_service/internal/models"
)

// FixedClock is a deterministic, concurrency-safe Clock.
type FixedClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFixedClock returns a clock pinned to a stable instant.
func NewFixedClock(now time.Time) *FixedClock {
	return &FixedClock{now: now.UTC()}
}

// Now returns the current fixed instant.
func (c *FixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d and returns the new instant.
func (c *FixedClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

// Set replaces the current instant.
func (c *FixedClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now.UTC()
}

var _ models.Clock = (*FixedClock)(nil)
