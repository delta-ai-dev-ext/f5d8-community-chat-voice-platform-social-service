package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"social_service/internal/conversation"
	"social_service/internal/events"
	"social_service/internal/friendship"
	"social_service/internal/httpapi"
	"social_service/internal/models"
	"social_service/internal/repository"
	"social_service/tests/mocks"
)

// baseTime is the injected clock instant for every integration test.
var baseTime = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

// harness wires the real router and domain services over the in-memory store,
// a fake Kafka broker, and a stubbed token verifier.
type harness struct {
	store    *repository.MemoryStore
	clock    *mocks.FixedClock
	broker   *mocks.FakeBroker
	verifier *mocks.StubVerifier
	probes   *httpapi.ProbeState
	registry *prometheus.Registry
	router   *gin.Engine
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	store := repository.NewMemoryStore()
	clock := mocks.NewFixedClock(baseTime)
	broker := mocks.NewFakeBroker()
	registry := prometheus.NewRegistry()
	logger := zap.NewNop()

	friendshipService := friendship.NewService(store, clock, logger, friendship.NewMetrics(registry))
	conversationService := conversation.NewService(store, clock, logger, conversation.NewMetrics(registry))
	events.NewMetrics(registry)
	probes := httpapi.NewProbeState(store.Ping, broker.Healthy)
	verifier := mocks.NewStubVerifier()

	router := httpapi.NewRouter(httpapi.Dependencies{
		Friendship:   friendshipService,
		Conversation: conversationService,
		Verifier:     verifier,
		Logger:       logger,
		Clock:        clock,
		Metrics:      httpapi.NewMetrics(registry),
		Probes:       probes,
		Registry:     registry,
	})
	t.Cleanup(store.Close)

	return &harness{
		store:    store,
		clock:    clock,
		broker:   broker,
		verifier: verifier,
		probes:   probes,
		registry: registry,
		router:   router,
	}
}

// tokenFor registers a bearer token for a user id.
func (h *harness) tokenFor(userID uuid.UUID) string {
	h.verifier.Allow("token-"+userID.String(), userID)
	return "token-" + userID.String()
}

// becomeFriends seeds friendships directly through the store.
func (h *harness) becomeFriends(t *testing.T, userID uuid.UUID, friends ...uuid.UUID) {
	t.Helper()
	for _, friend := range friends {
		lesser, greater := models.CanonicalPair(userID, friend)
		require.NoError(t, h.store.InTx(context.Background(), func(ctx context.Context, tx repository.Tx) error {
			return tx.InsertFriendship(ctx, models.Friendship{UserIDA: lesser, UserIDB: greater, CreatedAt: h.clock.Now()})
		}))
	}
}

// request performs one HTTP call. A nil body sends no body; an empty token
// omits the Authorization header.
func (h *harness) request(t *testing.T, method, path string, body any, token string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	var payload []byte
	switch typed := body.(type) {
	case nil:
	case []byte:
		payload = typed
	default:
		encoded, err := json.Marshal(typed)
		require.NoError(t, err)
		payload = encoded
	}

	request := httptest.NewRequest(method, path, bytes.NewReader(payload))
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}

	recorder := httptest.NewRecorder()
	h.router.ServeHTTP(recorder, request)
	return recorder
}

// errorBody decodes the {"error","detail"} envelope.
func errorBody(t *testing.T, recorder *httptest.ResponseRecorder) models.ErrorResponse {
	t.Helper()
	var body models.ErrorResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body), "body: %s", recorder.Body.String())
	return body
}

// decode unmarshals a JSON response body.
func decode[T any](t *testing.T, recorder *httptest.ResponseRecorder) T {
	t.Helper()
	var value T
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &value), "body: %s", recorder.Body.String())
	return value
}

// assertStatus asserts the status code and returns the decoded error body.
func (h *harness) assertStatus(t *testing.T, recorder *httptest.ResponseRecorder, status int, code models.ErrorCode) {
	t.Helper()
	require.Equal(t, status, recorder.Code, "body: %s", recorder.Body.String())
	if code != "" {
		body := errorBody(t, recorder)
		assert.Equal(t, string(code), body.Error)
		assert.NotEmpty(t, body.Detail)
	}
}

// sendFriendRequest creates a pending request and returns its id.
func (h *harness) sendFriendRequest(t *testing.T, sender, recipient uuid.UUID) models.FriendRequestDTO {
	t.Helper()
	recorder := h.request(t, http.MethodPost, "/friend-requests",
		map[string]string{"recipient_user_id": recipient.String()}, h.tokenFor(sender), nil)
	require.Equal(t, http.StatusCreated, recorder.Code, "body: %s", recorder.Body.String())
	return decode[models.FriendRequestDTO](t, recorder)
}

func TestProbesAndMetrics(t *testing.T) {
	t.Parallel()

	t.Run("healthy dependencies", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		assert.Equal(t, http.StatusOK, h.request(t, http.MethodGet, "/healthz", nil, "", nil).Code)
		assert.Equal(t, http.StatusOK, h.request(t, http.MethodGet, "/readyz", nil, "", nil).Code)
	})

	t.Run("healthz ignores dependency failures", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		h.store.SetPingError(repository.ErrUnavailable)
		h.broker.SetHealthy(false)

		assert.Equal(t, http.StatusOK, h.request(t, http.MethodGet, "/healthz", nil, "", nil).Code,
			"liveness never fails on PostgreSQL or Kafka hiccups")
		assert.Equal(t, http.StatusServiceUnavailable, h.request(t, http.MethodGet, "/readyz", nil, "", nil).Code)
	})

	t.Run("readyz fails during shutdown", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		require.Equal(t, http.StatusOK, h.request(t, http.MethodGet, "/readyz", nil, "", nil).Code)

		h.probes.BeginShutdown()
		assert.Equal(t, http.StatusServiceUnavailable, h.request(t, http.MethodGet, "/readyz", nil, "", nil).Code)
		assert.Equal(t, http.StatusServiceUnavailable, h.request(t, http.MethodGet, "/healthz", nil, "", nil).Code)
	})

	t.Run("metrics expose the documented series", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		user := uuid.New()
		h.sendFriendRequest(t, user, uuid.New())

		recorder := h.request(t, http.MethodGet, "/metrics", nil, "", nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		body := recorder.Body.String()
		for _, series := range []string{
			"http_requests_total",
			"http_request_duration_seconds",
			"social_friend_requests_total",
			"social_friendships_total",
			"social_conversations_created_total",
			"social_conversation_participants_added_total",
			"social_events_published_total",
			"social_outbox_unpublished",
		} {
			assert.Contains(t, body, series)
		}
		assert.Contains(t, body, `action="sent"`)
		assert.Contains(t, body, `route="/friend-requests"`, "route labels are templates, never raw ids")
	})
}

func TestAuthentication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header string
		token  string
		mutate func(h *harness)
	}{
		{name: "no authorization header"},
		{name: "wrong scheme", token: "token", mutate: func(h *harness) {}},
		{name: "empty bearer value", header: "Bearer "},
		{name: "basic scheme", header: "Basic dXNlcjpwYXNz"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			headers := map[string]string{}
			if test.header != "" {
				headers["Authorization"] = test.header
			}
			recorder := h.request(t, http.MethodGet, "/friends", nil, test.token, headers)
			h.assertStatus(t, recorder, http.StatusUnauthorized, models.CodeUnauthorized)
		})
	}

	t.Run("unknown token", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		recorder := h.request(t, http.MethodGet, "/friends", nil, "not-a-known-token", nil)
		h.assertStatus(t, recorder, http.StatusUnauthorized, models.CodeUnauthorized)
	})

	t.Run("verifier failure", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		h.verifier.FailAll()
		recorder := h.request(t, http.MethodGet, "/friends", nil, "token", nil)
		h.assertStatus(t, recorder, http.StatusUnauthorized, models.CodeUnauthorized)
	})

	t.Run("probes are unauthenticated", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		assert.Equal(t, http.StatusOK, h.request(t, http.MethodGet, "/healthz", nil, "", nil).Code)
		assert.Equal(t, http.StatusOK, h.request(t, http.MethodGet, "/readyz", nil, "", nil).Code)
		assert.Equal(t, http.StatusOK, h.request(t, http.MethodGet, "/metrics", nil, "", nil).Code)
	})

	t.Run("valid token is accepted", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		user := uuid.New()
		assert.Equal(t, http.StatusOK, h.request(t, http.MethodGet, "/friends", nil, h.tokenFor(user), nil).Code)
	})

	t.Run("unknown route", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		recorder := h.request(t, http.MethodGet, "/nope", nil, h.tokenFor(uuid.New()), nil)
		h.assertStatus(t, recorder, http.StatusNotFound, models.CodeNotFound)
	})
}
