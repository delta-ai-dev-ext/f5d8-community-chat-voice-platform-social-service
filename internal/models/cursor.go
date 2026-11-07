package models

import (
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidCursor is returned when an opaque cursor cannot be decoded.
var ErrInvalidCursor = errors.New("invalid cursor")

// Cursor is the keyset pagination position: rows are ordered by
// (created_at DESC, id DESC) and a page continues strictly after this position.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// Encode renders the cursor as an opaque URL-safe token.
func (c Cursor) Encode() string {
	raw := c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// ParseCursor decodes an opaque cursor. The empty string yields a zero cursor
// with ok=false and no error (first page).
func ParseCursor(raw string) (Cursor, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Cursor{}, false, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return Cursor{}, false, ErrInvalidCursor
	}
	parts := strings.Split(string(decoded), "|")
	if len(parts) != 2 {
		return Cursor{}, false, ErrInvalidCursor
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return Cursor{}, false, ErrInvalidCursor
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return Cursor{}, false, ErrInvalidCursor
	}
	return Cursor{CreatedAt: createdAt, ID: id}, true, nil
}

// Pagination limit bounds from the OpenAPI spec.
const (
	DefaultPageLimit = 50
	MaxPageLimit     = 100
)

// ResolveLimit applies the documented default (50) and maximum (100).
func ResolveLimit(requested int) (int, bool) {
	switch {
	case requested == 0:
		return DefaultPageLimit, true
	case requested < 0 || requested > MaxPageLimit:
		return 0, false
	default:
		return requested, true
	}
}
