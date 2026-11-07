// Package models holds the domain types, request/response DTOs, error
// taxonomy, and small cross-cutting helpers (clock, cursor, trace ids) shared
// by the domain services, the repository layer, and the HTTP boundary.
package models

import (
	"time"

	"github.com/google/uuid"
)

// FriendRequestStatus is the state of a friend request. The state machine is
// forward-only: pending -> accepted | declined | cancelled.
type FriendRequestStatus string

// Friend-request states.
const (
	FriendRequestPending   FriendRequestStatus = "pending"
	FriendRequestAccepted  FriendRequestStatus = "accepted"
	FriendRequestDeclined  FriendRequestStatus = "declined"
	FriendRequestCancelled FriendRequestStatus = "cancelled"
)

// Valid reports whether the status is one of the four known states.
func (s FriendRequestStatus) Valid() bool {
	switch s {
	case FriendRequestPending, FriendRequestAccepted, FriendRequestDeclined, FriendRequestCancelled:
		return true
	default:
		return false
	}
}

// Terminal reports whether the status can no longer transition. Only pending
// requests may be accepted, declined, or cancelled.
func (s FriendRequestStatus) Terminal() bool {
	return s.Valid() && s != FriendRequestPending
}

// FriendRequest is one friend request row.
type FriendRequest struct {
	ID              uuid.UUID
	SenderUserID    uuid.UUID
	RecipientUserID uuid.UUID
	Status          FriendRequestStatus
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Friendship is the unordered pair record created by an acceptance. Rows are
// stored canonically with UserIDA < UserIDB (see ADR-001).
type Friendship struct {
	UserIDA   uuid.UUID
	UserIDB   uuid.UUID
	CreatedAt time.Time
}

// CanonicalPair orders two user ids the way friendships are stored.
func CanonicalPair(a, b uuid.UUID) (lesser, greater uuid.UUID) {
	if a.String() < b.String() {
		return a, b
	}
	return b, a
}

// PairKey renders the canonical "<lesser>|<greater>" key used for direct
// conversations and for looking up a friendship row.
func PairKey(a, b uuid.UUID) string {
	lesser, greater := CanonicalPair(a, b)
	return lesser.String() + "|" + greater.String()
}

// Other returns the user id on the other side of the pair.
func (f Friendship) Other(userID uuid.UUID) (uuid.UUID, bool) {
	switch userID {
	case f.UserIDA:
		return f.UserIDB, true
	case f.UserIDB:
		return f.UserIDA, true
	default:
		return uuid.Nil, false
	}
}

// ConversationKind distinguishes 1:1 conversations from group conversations.
type ConversationKind string

// Conversation kinds.
const (
	ConversationDirect ConversationKind = "direct"
	ConversationGroup  ConversationKind = "group"
)

// Valid reports whether the kind is known.
func (k ConversationKind) Valid() bool {
	return k == ConversationDirect || k == ConversationGroup
}

// Participant is one conversation_participant row. LeftAt is nil while the
// user is a current participant.
type Participant struct {
	ConversationID uuid.UUID
	UserID         uuid.UUID
	JoinedAt       time.Time
	LeftAt         *time.Time
}

// Active reports whether the participant has not left the conversation.
func (p Participant) Active() bool { return p.LeftAt == nil }

// Conversation is a private conversation with its participant rows (active and
// historical).
type Conversation struct {
	ID            uuid.UUID
	Kind          ConversationKind
	Name          *string
	CreatedBy     uuid.UUID
	DirectPairKey *string
	Version       int
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Participants  []Participant
}

// ActiveParticipants returns the current participant rows, ordered as loaded.
func (c *Conversation) ActiveParticipants() []Participant {
	active := make([]Participant, 0, len(c.Participants))
	for _, p := range c.Participants {
		if p.Active() {
			active = append(active, p)
		}
	}
	return active
}

// ActiveParticipantIDs returns the current participant user ids.
func (c *Conversation) ActiveParticipantIDs() []uuid.UUID {
	active := c.ActiveParticipants()
	ids := make([]uuid.UUID, 0, len(active))
	for _, p := range active {
		ids = append(ids, p.UserID)
	}
	return ids
}

// IsActiveParticipant reports whether userID is a current participant.
func (c *Conversation) IsActiveParticipant(userID uuid.UUID) bool {
	for _, p := range c.Participants {
		if p.UserID == userID && p.Active() {
			return true
		}
	}
	return false
}

// RequestDirection selects which side of a friend request the caller is on.
type RequestDirection string

// Friend-request listing directions.
const (
	DirectionIncoming RequestDirection = "incoming"
	DirectionOutgoing RequestDirection = "outgoing"
)

// ParseRequestDirection parses the direction query parameter. The empty string
// defaults to incoming.
func ParseRequestDirection(raw string) (RequestDirection, bool) {
	switch RequestDirection(raw) {
	case "":
		return DirectionIncoming, true
	case DirectionIncoming:
		return DirectionIncoming, true
	case DirectionOutgoing:
		return DirectionOutgoing, true
	default:
		return "", false
	}
}

// Event aggregate types stored in the outbox.
const (
	AggregateTypeFriendRequest = "friend_request"
	AggregateTypeConversation  = "conversation"
)
