package unit_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"social_service/internal/config"
	"social_service/internal/httpapi"
	"social_service/internal/models"
)

func TestFriendRequestStatusHelpers(t *testing.T) {
	t.Parallel()

	for _, status := range []models.FriendRequestStatus{
		models.FriendRequestPending,
		models.FriendRequestAccepted,
		models.FriendRequestDeclined,
		models.FriendRequestCancelled,
	} {
		assert.True(t, status.Valid(), string(status))
	}
	assert.False(t, models.FriendRequestStatus("unknown").Valid())

	assert.False(t, models.FriendRequestPending.Terminal())
	assert.True(t, models.FriendRequestAccepted.Terminal())
	assert.True(t, models.FriendRequestDeclined.Terminal())
	assert.True(t, models.FriendRequestCancelled.Terminal())
}

func TestConversationHelpers(t *testing.T) {
	t.Parallel()

	assert.True(t, models.ConversationDirect.Valid())
	assert.True(t, models.ConversationGroup.Valid())
	assert.False(t, models.ConversationKind("broadcast").Valid())

	joined := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	left := joined.Add(time.Hour)
	active := uuid.New()
	departed := uuid.New()

	assert.True(t, models.Participant{UserID: active, JoinedAt: joined}.Active())
	assert.False(t, models.Participant{UserID: departed, JoinedAt: joined, LeftAt: &left}.Active())

	conversation := models.Conversation{
		Participants: []models.Participant{
			{UserID: active, JoinedAt: joined},
			{UserID: departed, JoinedAt: joined, LeftAt: &left},
		},
	}
	assert.True(t, conversation.IsActiveParticipant(active))
	assert.False(t, conversation.IsActiveParticipant(departed))
	assert.Len(t, conversation.ActiveParticipants(), 1)
	assert.Equal(t, []uuid.UUID{active}, conversation.ActiveParticipantIDs())
}

func TestRealClockIsUTC(t *testing.T) {
	t.Parallel()

	now := models.RealClock{}.Now()
	assert.Equal(t, time.UTC, now.Location())
}

func TestStatusClass(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "2xx", httpapi.StatusClass(201))
	assert.Equal(t, "4xx", httpapi.StatusClass(422))
	assert.Equal(t, "5xx", httpapi.StatusClass(503))
	assert.Equal(t, "unknown", httpapi.StatusClass(999))
}

// TestLoadFromProcessEnvironment exercises the real os.Getenv path, so it must
// not run in parallel with the tests that mutate the process environment.
func TestLoadFromProcessEnvironment(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	encoded, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}))

	t.Setenv(config.EnvDatabaseURL, "postgres://social:social@localhost:5432/social?sslmode=disable")
	t.Setenv(config.EnvKafkaBrokers, "broker:9092")
	t.Setenv(config.EnvTokenVerificationKeys, publicPEM)
	t.Setenv(config.EnvTokenIssuer, "identity_service")
	t.Setenv(config.EnvTokenAudience, "social_service")
	t.Setenv(config.EnvHTTPPort, "8081")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 8081, cfg.HTTPPort)
	assert.Equal(t, []string{"broker:9092"}, cfg.KafkaBrokers)
	require.Len(t, cfg.TokenVerificationKeys, 1)

	// OSEnv is the production lookup.
	assert.Equal(t, "8081", config.OSEnv(config.EnvHTTPPort))
	assert.Empty(t, config.OSEnv("SOCIAL_SERVICE_NOT_SET"))
}

func TestLoadVerificationKeysReadsInlinePEM(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	encoded, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}))

	env := baseEnv(t)
	env[config.EnvTokenVerificationKeys] = publicPEM
	cfg, err := loadFrom(env)
	require.NoError(t, err)
	require.Len(t, cfg.TokenVerificationKeys, 1)
	assert.NotNil(t, cfg.TokenVerificationKeys[0].PublicKey)
}
