// Package repository owns all PostgreSQL access behind interfaces. Domain
// services run their rule checks and state changes inside a single Tx so the
// state row and its event_outbox row commit atomically (ADR-002).
package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"social_service/internal/models"
)

// Storage-level sentinel errors. Implementations translate driver errors into
// these so domain services stay free of pgx specifics.
var (
	// ErrNoRows reports that the requested row does not exist.
	ErrNoRows = errors.New("repository: no rows")
	// ErrUniqueViolation reports a violated unique constraint.
	ErrUniqueViolation = errors.New("repository: unique violation")
	// ErrCheckViolation reports a violated CHECK constraint.
	ErrCheckViolation = errors.New("repository: check violation")
	// ErrUnavailable reports that the database cannot be reached.
	ErrUnavailable = errors.New("repository: database unavailable")
)

// Tx is the transactional slice of persistence used by the domain services.
// Every method is a single parameterized statement (or statement group) and
// never returns internal driver errors.
type Tx interface {
	// Friend requests.
	InsertFriendRequest(ctx context.Context, request models.FriendRequest) error
	GetFriendRequest(ctx context.Context, id uuid.UUID) (*models.FriendRequest, error)
	LockFriendRequest(ctx context.Context, id uuid.UUID) (*models.FriendRequest, error)
	UpdateFriendRequestStatus(ctx context.Context, id uuid.UUID, status models.FriendRequestStatus, at time.Time) error
	PendingFriendRequestBetween(ctx context.Context, first, second uuid.UUID) (*models.FriendRequest, error)
	ListFriendRequests(ctx context.Context, userID uuid.UUID, direction models.RequestDirection, cursor *models.Cursor, limit int) ([]models.FriendRequest, error)

	// Friendships.
	InsertFriendship(ctx context.Context, friendship models.Friendship) error
	GetFriendship(ctx context.Context, first, second uuid.UUID) (*models.Friendship, error)
	DeleteFriendship(ctx context.Context, first, second uuid.UUID) (bool, error)
	ListFriendships(ctx context.Context, userID uuid.UUID, cursor *models.Cursor, limit int) ([]models.Friendship, error)
	CountFriendships(ctx context.Context) (int64, error)

	// Conversations.
	InsertConversation(ctx context.Context, conversation models.Conversation) error
	GetConversation(ctx context.Context, id uuid.UUID) (*models.Conversation, error)
	DirectConversationByPairKey(ctx context.Context, pairKey string) (*models.Conversation, error)
	ListConversations(ctx context.Context, userID uuid.UUID, cursor *models.Cursor, limit int) ([]models.Conversation, error)
	// UpsertParticipant inserts a participant row or reactivates one that has
	// left. It reports whether the participant became active as a result.
	UpsertParticipant(ctx context.Context, participant models.Participant) (bool, error)
	MarkParticipantLeft(ctx context.Context, conversationID, userID uuid.UUID, at time.Time) (bool, error)
	BumpConversationVersion(ctx context.Context, conversationID uuid.UUID, at time.Time) (int, error)
	UpdateConversationName(ctx context.Context, conversationID uuid.UUID, name string, at time.Time) (int, error)

	// Transactional outbox.
	InsertOutboxRecord(ctx context.Context, record models.OutboxRecord) error
	ListUnpublishedOutbox(ctx context.Context, limit int) ([]models.OutboxRecord, error)
	MarkOutboxPublished(ctx context.Context, ids []uuid.UUID, at time.Time) error
	CountUnpublishedOutbox(ctx context.Context) (int64, error)
	// PreviousOutboxEventID returns the most recent outbox event id for an
	// aggregate, used as the causation_id of the next event in the flow.
	PreviousOutboxEventID(ctx context.Context, aggregateType string, aggregateID uuid.UUID) (*uuid.UUID, error)

	// Idempotency records.
	GetIdempotencyRecord(ctx context.Context, userID uuid.UUID, endpoint, key string) (*models.IdempotencyRecord, error)
	InsertIdempotencyRecord(ctx context.Context, record models.IdempotencyRecord) error
}

// Store begins transactional units of work.
type Store interface {
	// InTx runs fn inside one transaction. A nil return commits; an error
	// rolls back and is returned unchanged.
	InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
	// Ping reports whether the database is reachable (used by /readyz).
	Ping(ctx context.Context) error
	// Close releases the connection pool.
	Close()
}

// PaginationFetchSize returns the limit plus one so callers can detect a next
// page without a second query.
func PaginationFetchSize(limit int) int {
	return limit + 1
}

// NextCursor returns the cursor for the last row of a page when the fetch
// returned more rows than the page size, or nil on the last page.
func NextCursor[T any](rows []T, limit int, key func(T) models.Cursor) *string {
	if limit <= 0 || len(rows) <= limit {
		return nil
	}
	cursor := key(rows[limit-1]).Encode()
	return &cursor
}
