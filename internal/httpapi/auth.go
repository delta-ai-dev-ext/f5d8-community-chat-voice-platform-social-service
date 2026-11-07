package httpapi

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"social_service/internal/config"
	"social_service/internal/models"
)

// TokenVerifier verifies an access token locally. The interface keeps handlers
// testable without key material.
type TokenVerifier interface {
	Verify(ctx context.Context, rawToken string) (models.Principal, error)
}

// JWTVerifier verifies identity_service-issued access tokens against the
// configured current + next public keys. No network calls happen at request
// time.
type JWTVerifier struct {
	keys     []config.VerificationKey
	issuer   string
	audience string
	clock    models.Clock
}

// NewJWTVerifier builds a verifier for the supplied keys.
func NewJWTVerifier(keys []config.VerificationKey, issuer, audience string, clock models.Clock) *JWTVerifier {
	if clock == nil {
		clock = models.RealClock{}
	}
	return &JWTVerifier{keys: keys, issuer: issuer, audience: audience, clock: clock}
}

// accessTokenClaims are the claims this service reads.
type accessTokenClaims struct {
	jwt.RegisteredClaims
	SessionID     string `json:"sid,omitempty"`
	EmailVerified *bool  `json:"email_verified,omitempty"`
}

var acceptedAlgorithms = []string{
	"RS256", "RS384", "RS512",
	"ES256", "ES384", "ES512",
	"EdDSA",
}

// Verify parses and validates a bearer token. Every configured key is tried so
// that a token signed with the rotated-in `next` key verifies before the old
// key is retired.
func (v *JWTVerifier) Verify(_ context.Context, rawToken string) (models.Principal, error) {
	if len(v.keys) == 0 {
		return models.Principal{}, models.UnauthorizedError("no token verification keys are configured")
	}
	for _, candidate := range v.keys {
		claims := &accessTokenClaims{}
		token, err := jwt.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (any, error) {
			return keyForAlgorithm(token.Method.Alg(), candidate)
		},
			jwt.WithValidMethods(acceptedAlgorithms),
			jwt.WithIssuer(v.issuer),
			jwt.WithAudience(v.audience),
			jwt.WithExpirationRequired(),
			jwt.WithTimeFunc(v.clock.Now),
		)
		if err != nil || !token.Valid {
			continue
		}
		userID, err := uuid.Parse(claims.Subject)
		if err != nil {
			return models.Principal{}, models.UnauthorizedError("access token subject is not a user id")
		}
		principal := models.Principal{UserID: userID, SessionID: claims.SessionID}
		if claims.EmailVerified != nil {
			principal.EmailVerified = *claims.EmailVerified
		}
		return principal, nil
	}
	return models.Principal{}, models.UnauthorizedError("access token is invalid or expired")
}

// keyForAlgorithm returns the candidate key when its type matches the token's
// signing algorithm.
func keyForAlgorithm(algorithm string, candidate config.VerificationKey) (any, error) {
	switch algorithm {
	case "RS256", "RS384", "RS512":
		if _, ok := candidate.PublicKey.(*rsa.PublicKey); ok {
			return candidate.PublicKey, nil
		}
	case "ES256", "ES384", "ES512":
		if _, ok := candidate.PublicKey.(*ecdsa.PublicKey); ok {
			return candidate.PublicKey, nil
		}
	case "EdDSA":
		if _, ok := candidate.PublicKey.(ed25519.PublicKey); ok {
			return candidate.PublicKey, nil
		}
	}
	return nil, fmt.Errorf("verification key %q does not match algorithm %q", candidate.KeyID, algorithm)
}

// ErrInvalidToken is returned for a missing or malformed Authorization header.
var ErrInvalidToken = errors.New("invalid authorization header")

// bearerToken extracts the token from an `Authorization: Bearer <token>` header
// and rejects empty tokens.
func bearerToken(header string) (string, error) {
	const prefix = "bearer "
	if len(header) <= len(prefix) {
		return "", ErrInvalidToken
	}
	if !equalFoldASCII(header[:len(prefix)], prefix) {
		return "", ErrInvalidToken
	}
	token := header[len(prefix):]
	if token == "" {
		return "", ErrInvalidToken
	}
	return token, nil
}

// equalFoldASCII compares ASCII strings case-insensitively.
func equalFoldASCII(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := 0; i < len(left); i++ {
		lowerLeft := left[i]
		if lowerLeft >= 'A' && lowerLeft <= 'Z' {
			lowerLeft += 'a' - 'A'
		}
		lowerRight := right[i]
		if lowerRight >= 'A' && lowerRight <= 'Z' {
			lowerRight += 'a' - 'A'
		}
		if lowerLeft != lowerRight {
			return false
		}
	}
	return true
}
