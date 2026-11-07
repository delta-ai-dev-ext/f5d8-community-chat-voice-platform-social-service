package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"social_service/internal/config"
	"social_service/internal/conversation"
	"social_service/internal/events"
	"social_service/internal/friendship"
	"social_service/internal/httpapi"
	"social_service/internal/repository"
	"social_service/tests/mocks"
)

// baseTime is the injected clock instant for every entrypoint test.
var baseTime = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

func TestExecuteRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	// DATABASE_URL carries a password and KAFKA_BROKERS is valid, so the only
	// failures are the two missing token variables.
	values := map[string]string{
		config.EnvDatabaseURL:     "postgres://social:hunter2@localhost:5432/social?sslmode=disable",
		config.EnvKafkaBrokers:    "localhost:9092",
		config.EnvHTTPPort:        "8080",
		config.EnvShutdownTimeout: "30",
	}
	var out bytes.Buffer

	code := execute(context.Background(), func(key string) string { return values[key] }, &out)

	require.Equal(t, 1, code, "a rejected configuration must exit non-zero")
	entry := findLogEntry(t, decodeLogLines(t, out.String()), "operation", "startup")
	assert.Equal(t, "error", entry["status"])
	assert.Equal(t, "social_service", entry["service"])
	assert.NotEmpty(t, entry["trace_id"], "every log line carries a trace_id")

	detail, ok := entry["detail"].(string)
	require.True(t, ok, "the failure detail is a string: %v", entry["detail"])
	for _, name := range []string{config.EnvTokenIssuer, config.EnvTokenAudience} {
		assert.Contains(t, detail, name, "startup names the variable that is missing")
	}
	assert.NotContains(t, out.String(), "hunter2", "startup never prints a secret value")
	assert.NotContains(t, out.String(), "postgres://", "startup never prints a configured value")
}

func TestNewLoggerHonoursConfiguredLevel(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger, err := newLogger("error", &out)
	require.NoError(t, err)
	defer func() { _ = logger.Sync() }()

	logger.Info("suppressed at error level")
	logger.Error("emitted at error level")
	require.NoError(t, logger.Sync())

	assert.NotContains(t, out.String(), "suppressed at error level")
	entry := findLogEntry(t, decodeLogLines(t, out.String()), "msg", "emitted at error level")
	assert.Equal(t, "error", entry["level"])
	assert.NotEmpty(t, entry["ts"], "the production encoder stamps every line")
}

func TestNewLoggerRejectsUnsupportedLevel(t *testing.T) {
	t.Parallel()

	_, err := newLogger("verbose", io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verbose")
}

func TestServeShutsDownGracefully(t *testing.T) {
	t.Parallel()

	svc, broker, probes := newTestService(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	served := make(chan error, 1)
	go func() {
		served <- serve(ctx, svc)
	}()

	baseURL := "http://" + svc.listener.Addr().String()
	require.Eventually(t, func() bool {
		return statusOf(t, baseURL+"/healthz") == http.StatusOK
	}, 5*time.Second, 10*time.Millisecond, "the server must start serving")
	assert.Equal(t, http.StatusOK, statusOf(t, baseURL+"/readyz"))

	cancel()

	select {
	case err := <-served:
		require.NoError(t, err, "a signal-driven shutdown is a clean exit")
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return within the configured shutdown timeout")
	}

	assert.True(t, broker.Closed(), "the producer is flushed and closed before the pool")
	require.Error(t, probes.Liveness(), "readiness is flipped to failing once shutdown starts")
	_, err := net.DialTimeout("tcp", svc.listener.Addr().String(), 200*time.Millisecond)
	assert.Error(t, err, "the listener stops accepting new connections")
}

func TestServeReportsListenerFailure(t *testing.T) {
	t.Parallel()

	svc, _, _ := newTestService(t)
	require.NoError(t, svc.listener.Close())

	err := serve(context.Background(), svc)

	require.Error(t, err, "a dead listener must not look like a clean shutdown")
	assert.Contains(t, err.Error(), "closed")
}

// newTestService builds a servable dependency graph over the in-memory store and
// the fake broker, plus the 127.0.0.1:0 listener it serves on.
func newTestService(t *testing.T) (service, *mocks.FakeBroker, *httpapi.ProbeState) {
	t.Helper()

	clock := mocks.NewFixedClock(baseTime)
	store := repository.NewMemoryStore()
	broker := mocks.NewFakeBroker()
	registry := prometheus.NewRegistry()
	logger := zap.NewNop()

	publisher := events.NewPublisher(store, broker, clock, logger, events.NewMetrics(registry))
	probes := httpapi.NewProbeState(store.Ping, broker.Healthy)
	router := httpapi.NewRouter(httpapi.Dependencies{
		Friendship:   friendship.NewService(store, clock, logger, friendship.NewMetrics(registry)),
		Conversation: conversation.NewService(store, clock, logger, conversation.NewMetrics(registry)),
		Verifier:     mocks.NewStubVerifier(),
		Logger:       logger,
		Clock:        clock,
		Metrics:      httpapi.NewMetrics(registry),
		Probes:       probes,
		Registry:     registry,
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(store.Close)

	return service{
		config:    &config.Config{ShutdownTimeout: 5 * time.Second},
		logger:    logger,
		listener:  listener,
		router:    router,
		store:     store,
		broker:    broker,
		publisher: publisher,
		probes:    probes,
	}, broker, probes
}

// statusOf performs one GET and returns the status code, or 0 when the request
// fails because the server is not serving yet.
func statusOf(t *testing.T, url string) int {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		return 0
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

// decodeLogLines parses the JSON objects from the captured log output.
func decodeLogLines(t *testing.T, raw string) []map[string]any {
	t.Helper()

	var entries []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		entry := map[string]any{}
		require.NoError(t, json.Unmarshal([]byte(line), &entry), "log line is not JSON: %q", line)
		entries = append(entries, entry)
	}
	return entries
}

// findLogEntry returns the single entry whose field equals value.
func findLogEntry(t *testing.T, entries []map[string]any, field, value string) map[string]any {
	t.Helper()

	var matches []map[string]any
	for _, entry := range entries {
		if entry[field] == value {
			matches = append(matches, entry)
		}
	}
	require.Len(t, matches, 1, "expected exactly one log entry with %s=%q in %v", field, value, entries)
	return matches[0]
}
