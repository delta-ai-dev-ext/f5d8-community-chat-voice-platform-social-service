package unit_test

import (
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"social_service/internal/events"
	"social_service/internal/models"
)

var (
	friendRequestID = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	senderID        = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	recipientID     = uuid.MustParse("33333333-3333-4333-8333-333333333333")
	conversationID  = uuid.MustParse("44444444-4444-4444-8444-444444444444")
	firstMemberID   = uuid.MustParse("55555555-5555-4555-8555-555555555555")
)

const fixedTraceID = "abcdefabcdefabcdefabcdefabcdefab"

// envelopeKeys is the exact mandatory envelope field set.
var envelopeKeys = []string{
	"causation_id", "correlation_id", "event_id", "event_type", "event_version",
	"occurred_at", "producer", "schema_ref", "trace_id",
}

func TestFriendRequestMessageWireShape(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	record, err := events.NewFriendRequestOutbox(events.FriendRequestEvent{
		RequestID:       friendRequestID,
		SenderUserID:    senderID,
		RecipientUserID: recipientID,
		Status:          models.FriendRequestPending,
		TraceID:         fixedTraceID,
		OccurredAt:      occurredAt,
	})
	require.NoError(t, err)
	assert.Equal(t, recipientID.String(), record.PartitionKey)
	assert.Equal(t, events.Topic, record.Topic)
	assert.Equal(t, events.EventVersion, record.EventVersion)

	message, err := events.BuildMessage(*record, occurredAt)
	require.NoError(t, err)

	wire, err := message.Marshal()
	require.NoError(t, err)

	var document map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(wire, &document))
	require.Len(t, document, 2, "a message has exactly envelope and payload at the top level")
	assert.Contains(t, document, "envelope")
	assert.Contains(t, document, "payload")

	var envelope map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(document["envelope"], &envelope))
	assert.Equal(t, envelopeKeys, sortedKeys(envelope), "the envelope carries exactly the nine mandatory fields")

	assert.Equal(t, `"`+record.ID.String()+`"`, string(envelope["event_id"]))
	assert.Equal(t, `"friend_request_changed"`, string(envelope["event_type"]))
	assert.Equal(t, "1", string(envelope["event_version"]))
	assert.Equal(t, `"2026-07-30T12:00:00Z"`, string(envelope["occurred_at"]))
	assert.Equal(t, `"social_service"`, string(envelope["producer"]))
	assert.Equal(t, `"`+fixedTraceID+`"`, string(envelope["trace_id"]))
	assert.Equal(t, `"`+events.CorrelationFor(friendRequestID).String()+`"`, string(envelope["correlation_id"]))
	assert.Equal(t, "null", string(envelope["causation_id"]), "the first event of a flow has causation_id = null")
	assert.Equal(t, `"EVENT_CONTRACTS.md#friend_request_changed-v1"`, string(envelope["schema_ref"]))

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(document["payload"], &payload))
	assert.Equal(t, []string{"recipient_user_id", "request_id", "sender_user_id", "status"}, sortedKeys(payload))
	assert.Equal(t, `"`+friendRequestID.String()+`"`, string(payload["request_id"]))
	assert.Equal(t, `"`+senderID.String()+`"`, string(payload["sender_user_id"]))
	assert.Equal(t, `"`+recipientID.String()+`"`, string(payload["recipient_user_id"]))
	assert.Equal(t, `"pending"`, string(payload["status"]))
	assertNoEnvelopeFieldNames(t, payload)

	headers := message.Headers()
	assert.Equal(t, fixedTraceID, headers[events.HeaderTraceID])
	assert.Equal(t, "friend_request_changed", headers[events.HeaderEventType])
}

func TestConversationMessageWireShape(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 7, 30, 12, 30, 0, 0, time.UTC)
	name := "Book Club"
	causation := uuid.MustParse("66666666-6666-4666-8666-666666666666")
	record, err := events.NewConversationOutbox(events.ConversationEvent{
		ConversationID: conversationID,
		Kind:           models.ConversationGroup,
		Change:         events.ChangeRenamed,
		Name:           &name,
		ParticipantIDs: []uuid.UUID{senderID, firstMemberID},
		RemovedUserIDs: []uuid.UUID{firstMemberID},
		Version:        4,
		TraceID:        fixedTraceID,
		CausationID:    &causation,
		OccurredAt:     occurredAt,
	})
	require.NoError(t, err)
	assert.Equal(t, conversationID.String(), record.PartitionKey)

	message, err := events.BuildMessage(*record, occurredAt)
	require.NoError(t, err)
	wire, err := message.Marshal()
	require.NoError(t, err)

	var document struct {
		Envelope map[string]json.RawMessage `json:"envelope"`
		Payload  map[string]json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(wire, &document))
	assert.Equal(t, envelopeKeys, sortedKeys(document.Envelope))
	assert.Equal(t, `"`+causation.String()+`"`, string(document.Envelope["causation_id"]))
	assert.Equal(t, `"EVENT_CONTRACTS.md#conversation_changed-v1"`, string(document.Envelope["schema_ref"]))

	assert.Equal(t, []string{
		"added_user_ids", "change", "conversation_id", "conversation_version",
		"kind", "name", "participant_user_ids", "removed_user_ids",
	}, sortedKeys(document.Payload))
	assert.Equal(t, `"renamed"`, string(document.Payload["change"]))
	assert.Equal(t, `"group"`, string(document.Payload["kind"]))
	assert.Equal(t, `"Book Club"`, string(document.Payload["name"]))
	assert.Equal(t, "4", string(document.Payload["conversation_version"]))
	assert.Equal(t, `["`+senderID.String()+`","`+firstMemberID.String()+`"]`, string(document.Payload["participant_user_ids"]))
	assert.Equal(t, `[]`, string(document.Payload["added_user_ids"]), "arrays are always present, never null")
	assert.Equal(t, `["`+firstMemberID.String()+`"]`, string(document.Payload["removed_user_ids"]))
	assertNoEnvelopeFieldNames(t, document.Payload)
}

func TestBuildMessageNeverEmitsEmptyTraceID(t *testing.T) {
	t.Parallel()

	record, err := events.NewFriendRequestOutbox(events.FriendRequestEvent{
		RequestID:       friendRequestID,
		SenderUserID:    senderID,
		RecipientUserID: recipientID,
		Status:          models.FriendRequestPending,
		OccurredAt:      time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// A stored trace id that is missing or malformed is minted at the edge.
	record.TraceID = ""
	message, err := events.BuildMessage(*record, time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.True(t, models.ValidTraceID(message.Envelope.TraceID))
	assert.Equal(t, 32, len(message.Envelope.TraceID))

	record.TraceID = "NOT-A-TRACE-ID"
	message, err = events.BuildMessage(*record, time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.True(t, models.ValidTraceID(message.Envelope.TraceID))
}

func TestBuildMessageRejectsMalformedRecords(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	valid, err := events.NewFriendRequestOutbox(events.FriendRequestEvent{
		RequestID:       friendRequestID,
		SenderUserID:    senderID,
		RecipientUserID: recipientID,
		Status:          models.FriendRequestPending,
		TraceID:         fixedTraceID,
		OccurredAt:      occurredAt,
	})
	require.NoError(t, err)

	tests := []struct {
		name   string
		mutate func(record *models.OutboxRecord)
	}{
		{"missing id", func(record *models.OutboxRecord) { record.ID = uuid.Nil }},
		{"missing event type", func(record *models.OutboxRecord) { record.EventType = "" }},
		{"invalid version", func(record *models.OutboxRecord) { record.EventVersion = 0 }},
		{"invalid payload", func(record *models.OutboxRecord) { record.Payload = []byte("{") }},
		{"empty payload", func(record *models.OutboxRecord) { record.Payload = nil }},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			record := *valid
			test.mutate(&record)
			_, err := events.BuildMessage(record, occurredAt)
			require.Error(t, err)
			assert.ErrorIs(t, err, events.ErrInvalidEvent)
		})
	}
}

func TestOutboxRecordConstructorsValidateInput(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

	_, err := events.NewFriendRequestOutbox(events.FriendRequestEvent{
		RequestID: friendRequestID, SenderUserID: senderID, RecipientUserID: recipientID,
		Status: "unknown", OccurredAt: occurredAt,
	})
	require.Error(t, err)

	_, err = events.NewConversationOutbox(events.ConversationEvent{
		ConversationID: conversationID, Kind: "unknown", Change: events.ChangeCreated,
		Version: 1, OccurredAt: occurredAt,
	})
	require.Error(t, err)

	_, err = events.NewConversationOutbox(events.ConversationEvent{
		ConversationID: conversationID, Kind: models.ConversationGroup, Change: "unknown",
		Version: 1, OccurredAt: occurredAt,
	})
	require.Error(t, err)

	_, err = events.NewConversationOutbox(events.ConversationEvent{
		ConversationID: conversationID, Kind: models.ConversationGroup, Change: events.ChangeCreated,
		Version: 0, OccurredAt: occurredAt,
	})
	require.Error(t, err)
}

func TestCorrelationIsStablePerFlow(t *testing.T) {
	t.Parallel()

	first := events.CorrelationFor(friendRequestID)
	second := events.CorrelationFor(friendRequestID)
	assert.Equal(t, first, second)
	assert.NotEqual(t, first, events.CorrelationFor(conversationID))
}

func sortedKeys(document map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(document))
	for key := range document {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// assertNoEnvelopeFieldNames enforces the rule that a payload must not declare
// an envelope field name at its top level.
func assertNoEnvelopeFieldNames(t *testing.T, payload map[string]json.RawMessage) {
	t.Helper()
	for _, key := range envelopeKeys {
		assert.NotContains(t, payload, key, "payload must not carry envelope field %q", key)
	}
}
