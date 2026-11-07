package unit_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"social_service/internal/config"
	"social_service/internal/httpapi"
	"social_service/internal/models"
	"social_service/tests/mocks"
)

const (
	testIssuer   = "identity_service"
	testAudience = "social_service"
)

type signer struct {
	method jwt.SigningMethod
	key    any
	keyID  string
}

func newRSASigner(t *testing.T) signer {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return signer{method: jwt.SigningMethodRS256, key: privateKey, keyID: "current"}
}

func newEd25519Signer(t *testing.T) signer {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return signer{method: jwt.SigningMethodEdDSA, key: privateKey, keyID: "next"}
}

func newECDSASigner(t *testing.T) signer {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return signer{method: jwt.SigningMethodES256, key: privateKey, keyID: "current"}
}

// verificationKey derives the public verification key for a signer.
func verificationKey(t *testing.T, s signer) config.VerificationKey {
	t.Helper()
	var publicKey any
	switch key := s.key.(type) {
	case *rsa.PrivateKey:
		publicKey = &key.PublicKey
	case ed25519.PrivateKey:
		publicKey = key.Public()
	case *ecdsa.PrivateKey:
		publicKey = &key.PublicKey
	default:
		t.Fatalf("unsupported key type %T", s.key)
	}
	return config.VerificationKey{KeyID: s.keyID, PublicKey: publicKey}
}

func signToken(t *testing.T, s signer, mutate func(claims jwt.MapClaims)) string {
	t.Helper()
	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"sub":            uuid.New().String(),
		"sid":            "session-1",
		"email_verified": false,
		"iss":            testIssuer,
		"aud":            testAudience,
		"iat":            now.Unix(),
		"exp":            now.Add(15 * time.Minute).Unix(),
	}
	if mutate != nil {
		mutate(claims)
	}
	token := jwt.NewWithClaims(s.method, claims)
	signed, err := token.SignedString(s.key)
	require.NoError(t, err)
	return signed
}

func TestJWTVerifierAcceptsValidTokens(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		signer func(t *testing.T) signer
	}{
		{name: "rs256", signer: newRSASigner},
		{name: "eddsa", signer: newEd25519Signer},
		{name: "es256", signer: newECDSASigner},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			s := test.signer(t)
			clock := mocks.NewFixedClock(occurredAt)
			verifier := httpapi.NewJWTVerifier([]config.VerificationKey{verificationKey(t, s)}, testIssuer, testAudience, clock)

			userID := uuid.New()
			token := signToken(t, s, func(claims jwt.MapClaims) {
				claims["sub"] = userID.String()
				claims["exp"] = occurredAt.Add(5 * time.Minute).Unix()
				claims["iat"] = occurredAt.Unix()
				claims["email_verified"] = true
			})

			principal, err := verifier.Verify(context.Background(), token)
			require.NoError(t, err)
			assert.Equal(t, userID, principal.UserID)
			assert.Equal(t, "session-1", principal.SessionID)
			assert.True(t, principal.EmailVerified)
		})
	}
}

func TestJWTVerifierRotatesKeys(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	current := newRSASigner(t)
	next := newRSASigner(t)
	verifier := httpapi.NewJWTVerifier(
		[]config.VerificationKey{verificationKey(t, current), verificationKey(t, next)},
		testIssuer, testAudience, mocks.NewFixedClock(occurredAt),
	)

	// A token signed by the next key (already published) verifies too.
	token := signToken(t, next, func(claims jwt.MapClaims) {
		claims["exp"] = occurredAt.Add(time.Minute).Unix()
	})
	_, err := verifier.Verify(context.Background(), token)
	require.NoError(t, err)
}

func TestJWTVerifierRejectsInvalidTokens(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	current := newRSASigner(t)
	untrusted := newRSASigner(t)

	tests := []struct {
		name   string
		token  func(t *testing.T) string
		detail string
	}{
		{
			name: "expired",
			token: func(t *testing.T) string {
				return signToken(t, current, func(claims jwt.MapClaims) {
					claims["exp"] = occurredAt.Add(-time.Minute).Unix()
				})
			},
		},
		{
			name: "missing expiration",
			token: func(t *testing.T) string {
				return signToken(t, current, func(claims jwt.MapClaims) { delete(claims, "exp") })
			},
		},
		{
			name: "wrong issuer",
			token: func(t *testing.T) string {
				return signToken(t, current, func(claims jwt.MapClaims) {
					claims["iss"] = "someone_else"
					claims["exp"] = occurredAt.Add(time.Minute).Unix()
				})
			},
		},
		{
			name: "wrong audience",
			token: func(t *testing.T) string {
				return signToken(t, current, func(claims jwt.MapClaims) {
					claims["aud"] = "another_service"
					claims["exp"] = occurredAt.Add(time.Minute).Unix()
				})
			},
		},
		{
			name: "untrusted key",
			token: func(t *testing.T) string {
				return signToken(t, untrusted, func(claims jwt.MapClaims) {
					claims["exp"] = occurredAt.Add(time.Minute).Unix()
				})
			},
		},
		{
			name: "subject is not a user id",
			token: func(t *testing.T) string {
				return signToken(t, current, func(claims jwt.MapClaims) {
					claims["sub"] = "not-a-uuid"
					claims["exp"] = occurredAt.Add(time.Minute).Unix()
				})
			},
		},
		{
			name:  "garbage token",
			token: func(*testing.T) string { return "not.a.jwt" },
		},
	}

	verifier := httpapi.NewJWTVerifier([]config.VerificationKey{verificationKey(t, current)}, testIssuer, testAudience, mocks.NewFixedClock(occurredAt))
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := verifier.Verify(context.Background(), test.token(t))
			require.Error(t, err)
			domainErr, ok := models.AsDomainError(err)
			require.True(t, ok)
			assert.Equal(t, models.CodeUnauthorized, domainErr.Code)
			assert.Equal(t, 401, domainErr.Status)
		})
	}
}

func TestJWTVerifierWithoutKeysRejectsEverything(t *testing.T) {
	t.Parallel()

	verifier := httpapi.NewJWTVerifier(nil, testIssuer, testAudience, mocks.NewFixedClock(time.Now()))
	_, err := verifier.Verify(context.Background(), signToken(t, newRSASigner(t), nil))
	require.Error(t, err)
}
