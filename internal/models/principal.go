package models

import "github.com/google/uuid"

// Principal is the authenticated caller derived from a verified access token.
type Principal struct {
	// UserID is the token's `sub` claim.
	UserID uuid.UUID
	// SessionID is the token's `sid` claim when present.
	SessionID string
	// EmailVerified mirrors the token's `email_verified` claim. It never gates
	// access in this service (that is identity_service's concern).
	EmailVerified bool
}
