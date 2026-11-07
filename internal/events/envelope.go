// Package events owns the mandatory event envelope, the EVT-007 / EVT-008
// payloads, transactional-outbox record construction, and the Kafka publisher
// loop.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"social_service/internal/models"
)

// Producer identity of this service, mirrored in every envelope.
const Producer = "social_service"

// Topic is the single topic this service produces on.
const Topic = "social_events"

// Event types (contract registry rows DEP-010 / DEP-011).
const (
	EventTypeFriendRequestChanged = "friend_request_changed"
	EventTypeConversationChanged  = "conversation_changed"
)

// EventVersion is the current version of both contracts.
const EventVersion = 1

// Conversation change values carried by EVT-008 payloads.
const (
	ChangeCreated             = "created"
	ChangeParticipantsChanged = "participants_changed"
	ChangeRenamed             = "renamed"
)

// Kafka header names mirroring envelope fields.
const (
	HeaderTraceID   = "trace_id"
	HeaderEventType = "event_type"
)

// ErrInvalidEvent is returned when an outbox record cannot be turned into a
// well-formed message.
var ErrInvalidEvent = errors.New("events: invalid outbox record")

// SchemaRef renders the contract reference for an event type and version.
func SchemaRef(eventType string, version int) string {
	return fmt.Sprintf("EVENT_CONTRACTS.md#%s-v%d", eventType, version)
}

// Envelope is the mandatory envelope every event carries. It is modelled once
// so no publish site can omit a field (notably trace_id).
type Envelope struct {
	EventID       string  `json:"event_id"`
	EventType     string  `json:"event_type"`
	EventVersion  int     `json:"event_version"`
	OccurredAt    string  `json:"occurred_at"`
	Producer      string  `json:"producer"`
	TraceID       string  `json:"trace_id"`
	CorrelationID string  `json:"correlation_id"`
	CausationID   *string `json:"causation_id"`
	SchemaRef     string  `json:"schema_ref"`
}

// Message is the wire shape of a Kafka message: exactly `envelope` and
// `payload` at the top level.
type Message struct {
	Envelope Envelope        `json:"envelope"`
	Payload  json.RawMessage `json:"payload"`
}

// Marshal renders the message for the broker.
func (m Message) Marshal() ([]byte, error) {
	encoded, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal event message: %w", err)
	}
	return encoded, nil
}

// Headers returns the mirrored envelope fields carried as Kafka headers.
func (m Message) Headers() map[string]string {
	return map[string]string{
		HeaderTraceID:   m.Envelope.TraceID,
		HeaderEventType: m.Envelope.EventType,
	}
}

// BuildMessage assembles the message for an outbox record. failed over to a
// minted id when the recorded one is empty, so trace_id is never empty.
func BuildMessage(record models.OutboxRecord, occurredAt time.Time) (Message, error) {
	if record.ID == (models.OutboxRecord{}).ID {
		return Message{}, fmt.Errorf("%w: missing event id", ErrInvalidEvent)
	}
	if record.EventType == "" {
		return Message{}, fmt.Errorf("%w: missing event type", ErrInvalidEvent)
	}
	if record.EventVersion < 1 {
		return Message{}, fmt.Errorf("%w: event version must be >= 1", ErrInvalidEvent)
	}
	if len(record.Payload) == 0 || !json.Valid(record.Payload) {
		return Message{}, fmt.Errorf("%w: payload is not valid JSON", ErrInvalidEvent)
	}
	traceID := record.TraceID
	if !models.ValidTraceID(traceID) {
		// Never emit an empty trace id; mint at the edge when the stored one is
		// unusable.
		traceID = models.NewTraceID()
	}
	var causationID *string
	if record.CausationID != nil {
		value := record.CausationID.String()
		causationID = &value
	}
	return Message{
		Envelope: Envelope{
			EventID:       record.ID.String(),
			EventType:     record.EventType,
			EventVersion:  record.EventVersion,
			OccurredAt:    occurredAt.UTC().Format(time.RFC3339Nano),
			Producer:      Producer,
			TraceID:       traceID,
			CorrelationID: record.CorrelationID.String(),
			CausationID:   causationID,
			SchemaRef:     SchemaRef(record.EventType, record.EventVersion),
		},
		Payload: json.RawMessage(record.Payload),
	}, nil
}
