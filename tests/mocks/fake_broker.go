package mocks

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"social_service/internal/events"
)

// ErrBrokerFailure is returned by FakeBroker when failures are injected.
var ErrBrokerFailure = errors.New("mocks: broker failure")

// SentMessage is one recorded produce call.
type SentMessage struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

// DecodedMessage returns the recorded payload decoded into the envelope +
// payload wire shape.
func (m SentMessage) DecodedMessage() (events.Message, error) {
	var message events.Message
	if err := json.Unmarshal(m.Value, &message); err != nil {
		return events.Message{}, err
	}
	return message, nil
}

// FakeBroker is an in-process Broker double. It records every message and can
// be told to fail or to report itself unhealthy.
type FakeBroker struct {
	mu       sync.Mutex
	messages []SentMessage
	failures int
	closed   bool
	healthy  bool
	closedAt int
}

// NewFakeBroker returns a healthy fake broker.
func NewFakeBroker() *FakeBroker {
	return &FakeBroker{healthy: true}
}

// Send records a message unless a failure is pending.
func (b *FakeBroker) Send(_ context.Context, topic, key string, value []byte, headers map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("mocks: broker closed")
	}
	if b.failures > 0 {
		b.failures--
		return ErrBrokerFailure
	}
	copiedHeaders := make(map[string]string, len(headers))
	for name, header := range headers {
		copiedHeaders[name] = header
	}
	b.messages = append(b.messages, SentMessage{
		Topic:   topic,
		Key:     key,
		Value:   append([]byte(nil), value...),
		Headers: copiedHeaders,
	})
	return nil
}

// Healthy reports the configured health.
func (b *FakeBroker) Healthy() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.healthy
}

// Close marks the broker closed.
func (b *FakeBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.closedAt++
	return nil
}

// SetHealthy toggles the reported health.
func (b *FakeBroker) SetHealthy(healthy bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.healthy = healthy
}

// FailNext makes the next n Send calls fail.
func (b *FakeBroker) FailNext(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = n
}

// Messages returns a copy of the recorded messages.
func (b *FakeBroker) Messages() []SentMessage {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]SentMessage, len(b.messages))
	copy(out, b.messages)
	return out
}

// MessageCount returns how many messages were produced.
func (b *FakeBroker) MessageCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.messages)
}

// Closed reports whether Close was called.
func (b *FakeBroker) Closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

var _ events.Broker = (*FakeBroker)(nil)
