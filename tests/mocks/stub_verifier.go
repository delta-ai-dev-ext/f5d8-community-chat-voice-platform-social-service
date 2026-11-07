package mocks

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"

	"social_service/internal/httpapi"
	"social_service/internal/models"
)

// ErrStubUnauthorized is returned for tokens the stub does not know.
var ErrStubUnauthorized = errors.New("mocks: unknown token")

// StubVerifier is a TokenVerifier double. Tokens registered with Allow map to a
// principal; every other token is rejected.
type StubVerifier struct {
	mu         sync.Mutex
	principals map[string]models.Principal
	failAll    bool
}

// NewStubVerifier returns an empty stub.
func NewStubVerifier() *StubVerifier {
	return &StubVerifier{principals: map[string]models.Principal{}}
}

// Allow registers a token for a user id.
func (v *StubVerifier) Allow(token string, userID uuid.UUID) *StubVerifier {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.principals[token] = models.Principal{UserID: userID, SessionID: "session-stub"}
	return v
}

// FailAll makes every token invalid.
func (v *StubVerifier) FailAll() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.failAll = true
}

// Verify resolves a token to its principal.
func (v *StubVerifier) Verify(_ context.Context, rawToken string) (models.Principal, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.failAll {
		return models.Principal{}, ErrStubUnauthorized
	}
	principal, ok := v.principals[rawToken]
	if !ok {
		return models.Principal{}, ErrStubUnauthorized
	}
	return principal, nil
}

var _ httpapi.TokenVerifier = (*StubVerifier)(nil)
