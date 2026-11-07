package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"social_service/internal/models"
)

// flowNamespace is the fixed namespace used to derive a stable correlation id
// per business flow (one flow per friend request, one per conversation).
var flowNamespace = uuid.MustParse("6f1a1f4e-1f0f-5f2b-9d7a-1a2b3c4d5e6f")

// CorrelationFor returns the correlation id shared by every event of one
// business flow. Deriving it from the aggregate id keeps the flow identity
// stable without an extra column.
func CorrelationFor(aggregateID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(flowNamespace, aggregateID[:])
}

// FriendRequestPayload is the EVT-007 payload contract (verbatim).
type FriendRequestPayload struct {
	RequestID       string `json:"request_id"`
	SenderUserID    string `json:"sender_user_id"`
	RecipientUserID string `json:"recipient_user_id"`
	Status          string `json:"status"`
}

// ConversationPayload is the EVT-008 payload contract (verbatim).
type ConversationPayload struct {
	ConversationID      string   `json:"conversation_id"`
	Kind                string   `json:"kind"`
	Change              string   `json:"change"`
	Name                *string  `json:"name"`
	ParticipantUserIDs  []string `json:"participant_user_ids"`
	AddedUserIDs        []string `json:"added_user_ids"`
	RemovedUserIDs      []string `json:"removed_user_ids"`
	ConversationVersion int      `json:"conversation_version"`
}

// FriendRequestEvent describes one friend-request state change to publish.
type FriendRequestEvent struct {
	RequestID       uuid.UUID
	SenderUserID    uuid.UUID
	RecipientUserID uuid.UUID
	Status          models.FriendRequestStatus

	TraceID       string
	CorrelationID uuid.UUID
	CausationID   *uuid.UUID
	OccurredAt    time.Time
}

// NewFriendRequestOutbox builds the outbox row for EVT-007. The partition key
// is the recipient so notification ordering per recipient is preserved.
func NewFriendRequestOutbox(in FriendRequestEvent) (*models.OutboxRecord, error) {
	if !in.Status.Valid() {
		return nil, fmt.Errorf("%w: unknown friend request status %q", ErrInvalidEvent, in.Status)
	}
	payload, err := json.Marshal(FriendRequestPayload{
		RequestID:       in.RequestID.String(),
		SenderUserID:    in.SenderUserID.String(),
		RecipientUserID: in.RecipientUserID.String(),
		Status:          string(in.Status),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal friend_request_changed payload: %w", err)
	}
	return &models.OutboxRecord{
		ID:            uuid.New(),
		AggregateType: models.AggregateTypeFriendRequest,
		AggregateID:   in.RequestID,
		EventType:     EventTypeFriendRequestChanged,
		EventVersion:  EventVersion,
		Topic:         Topic,
		PartitionKey:  in.RecipientUserID.String(),
		CorrelationID: correlationOrDerived(in.CorrelationID, in.RequestID),
		CausationID:   in.CausationID,
		TraceID:       traceIDOrMint(in.TraceID),
		Payload:       payload,
		CreatedAt:     in.OccurredAt.UTC(),
	}, nil
}

// ConversationEvent describes one conversation change to publish.
type ConversationEvent struct {
	ConversationID uuid.UUID
	Kind           models.ConversationKind
	Change         string
	Name           *string
	ParticipantIDs []uuid.UUID
	AddedUserIDs   []uuid.UUID
	RemovedUserIDs []uuid.UUID
	Version        int

	TraceID       string
	CorrelationID uuid.UUID
	CausationID   *uuid.UUID
	OccurredAt    time.Time
}

// NewConversationOutbox builds the outbox row for EVT-008. The payload always
// carries the full current participant list plus the delta.
func NewConversationOutbox(in ConversationEvent) (*models.OutboxRecord, error) {
	if !in.Kind.Valid() {
		return nil, fmt.Errorf("%w: unknown conversation kind %q", ErrInvalidEvent, in.Kind)
	}
	switch in.Change {
	case ChangeCreated, ChangeParticipantsChanged, ChangeRenamed:
	default:
		return nil, fmt.Errorf("%w: unknown conversation change %q", ErrInvalidEvent, in.Change)
	}
	if in.Version < 1 {
		return nil, fmt.Errorf("%w: conversation version must be >= 1", ErrInvalidEvent)
	}
	payload, err := json.Marshal(ConversationPayload{
		ConversationID:      in.ConversationID.String(),
		Kind:                string(in.Kind),
		Change:              in.Change,
		Name:                in.Name,
		ParticipantUserIDs:  stringIDs(in.ParticipantIDs),
		AddedUserIDs:        stringIDs(in.AddedUserIDs),
		RemovedUserIDs:      stringIDs(in.RemovedUserIDs),
		ConversationVersion: in.Version,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal conversation_changed payload: %w", err)
	}
	return &models.OutboxRecord{
		ID:            uuid.New(),
		AggregateType: models.AggregateTypeConversation,
		AggregateID:   in.ConversationID,
		EventType:     EventTypeConversationChanged,
		EventVersion:  EventVersion,
		Topic:         Topic,
		PartitionKey:  in.ConversationID.String(),
		CorrelationID: correlationOrDerived(in.CorrelationID, in.ConversationID),
		CausationID:   in.CausationID,
		TraceID:       traceIDOrMint(in.TraceID),
		Payload:       payload,
		CreatedAt:     in.OccurredAt.UTC(),
	}, nil
}

func stringIDs(ids []uuid.UUID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	return out
}

func correlationOrDerived(correlationID, aggregateID uuid.UUID) uuid.UUID {
	if correlationID == uuid.Nil {
		return CorrelationFor(aggregateID)
	}
	return correlationID
}

func traceIDOrMint(traceID string) string {
	if models.ValidTraceID(traceID) {
		return traceID
	}
	return models.NewTraceID()
}
