package models

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

// OutboxRecord is one row of the transactional event_outbox. Envelope fields
// that are produced at publish time (event_id aside) are stored so the event
// survives a crash between commit and publish.
type OutboxRecord struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	EventVersion  int
	Topic         string
	PartitionKey  string
	CorrelationID uuid.UUID
	CausationID   *uuid.UUID
	TraceID       string
	Payload       []byte
	PublishedAt   *time.Time
	CreatedAt     time.Time
}

// MaxIdempotencyKeyLength mirrors the OpenAPI Idempotency-Key header schema.
const MaxIdempotencyKeyLength = 64

// IdempotencyKey is the client-supplied retry key plus the hash of the request
// body it was first used with.
type IdempotencyKey struct {
	Key         string
	RequestHash string
}

// IdempotencyRecord is the stored outcome of an idempotent creation request.
// Replays return StatusCode with ResourceID reloaded from storage, which is why
// only a reference (not the serialized body) is persisted.
type IdempotencyRecord struct {
	UserID      uuid.UUID
	Endpoint    string
	Key         string
	RequestHash string
	StatusCode  int
	ResourceID  uuid.UUID
	CreatedAt   time.Time
}

// HashRequestBody returns the SHA-256 hex digest used to detect reuse of an
// Idempotency-Key with a different body.
func HashRequestBody(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// NewIdempotencyKey builds a key value; an empty header means "no key".
func NewIdempotencyKey(key string, body []byte) *IdempotencyKey {
	if key == "" {
		return nil
	}
	return &IdempotencyKey{Key: key, RequestHash: HashRequestBody(body)}
}
