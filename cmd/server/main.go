// Command server is the social_service entrypoint. It loads and validates the
// 12-factor configuration, wires the HTTP API, the domain services, and the
// transactional-outbox publisher, then serves until SIGTERM/SIGINT and shuts
// down in the order architecture §6 prescribes.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"social_service/internal/config"
	"social_service/internal/conversation"
	"social_service/internal/events"
	"social_service/internal/friendship"
	"social_service/internal/httpapi"
	"social_service/internal/models"
	"social_service/internal/repository"
)

// serviceName is the identity this process logs and produces events under.
const serviceName = "social_service"

func main() {
	// .env is a local-development convenience only (engineering guide §2):
	// deployments inject the environment directly. godotenv never overrides a
	// variable that is already set, and a missing file is not an error.
	_ = godotenv.Load()

	os.Exit(execute(context.Background(), config.OSEnv, os.Stderr))
}

// execute is main's body in testable form: it returns the process exit code.
// out receives the structured logs, so a configuration failure can be asserted
// without starting PostgreSQL or Kafka.
func execute(ctx context.Context, getenv config.Getenv, out io.Writer) int {
	cfg, err := config.LoadFrom(getenv)
	if err != nil {
		// LOG_LEVEL is not trustworthy yet, so the failure is reported through a
		// logger at the default level. The message names missing variables and
		// never carries their values.
		logger, logErr := newLogger(config.DefaultLogLevel, out)
		if logErr != nil {
			_, _ = fmt.Fprintf(out, "%s: configuration is invalid: %v\n", serviceName, err)
			return 1
		}
		logStartupFailure(logger, err)
		return 1
	}

	logger, err := newLogger(cfg.LogLevel, out)
	if err != nil {
		_, _ = fmt.Fprintf(out, "%s: init logger: %v\n", serviceName, err)
		return 1
	}
	defer func() { _ = logger.Sync() }()

	// SIGTERM/SIGINT start the graceful-shutdown sequence in serve.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("service stopped with an error",
			zap.String("service", serviceName),
			zap.String("trace_id", models.NewTraceID()),
			zap.String("operation", "shutdown"),
			zap.String("status", "error"),
			zap.String("detail", err.Error()),
		)
		return 1
	}
	return 0
}

// run binds the listening socket, builds the dependency graph, and serves until
// ctx is cancelled. Startup failures close whatever was already opened.
func run(ctx context.Context, cfg *config.Config, logger *zap.Logger) error {
	clock := models.RealClock{}

	address := listenAddress(cfg.HTTPPort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}

	store, err := repository.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("open postgres pool: %w", err)
	}

	broker, err := events.NewKafkaBroker(cfg.Kafka(), logger)
	if err != nil {
		_ = listener.Close()
		store.Close()
		return fmt.Errorf("configure kafka producer: %w", err)
	}
	// An unreachable broker must not stop the process: HTTP writes still commit
	// state + outbox rows (architecture §6), the publisher retries with
	// back-off, and /readyz stays 503 until the producer connects.
	if err := broker.Connect(); err != nil {
		logger.Warn("kafka producer is not connected yet; readiness stays degraded and the outbox publisher retries",
			zap.String("service", serviceName),
			zap.String("trace_id", models.NewTraceID()),
			zap.String("operation", "kafka_connect"),
			zap.String("status", "degraded"),
			zap.String("detail", err.Error()),
		)
	}

	// One registry per process so /metrics exposes exactly the documented series.
	registry := prometheus.NewRegistry()
	httpMetrics := httpapi.NewMetrics(registry)
	eventMetrics := events.NewMetrics(registry)
	friendshipService := friendship.NewService(store, clock, logger, friendship.NewMetrics(registry))
	conversationService := conversation.NewService(store, clock, logger, conversation.NewMetrics(registry))
	publisher := events.NewPublisher(store, broker, clock, logger, eventMetrics)
	probes := httpapi.NewProbeState(store.Ping, broker.Healthy)
	verifier := httpapi.NewJWTVerifier(cfg.TokenVerificationKeys, cfg.TokenIssuer, cfg.TokenAudience, clock)
	router := httpapi.NewRouter(httpapi.Dependencies{
		Friendship:   friendshipService,
		Conversation: conversationService,
		Verifier:     verifier,
		Logger:       logger,
		Clock:        clock,
		Metrics:      httpMetrics,
		Probes:       probes,
		Registry:     registry,
	})

	return serve(ctx, service{
		config:    cfg,
		logger:    logger,
		listener:  listener,
		router:    router,
		store:     store,
		broker:    broker,
		publisher: publisher,
		probes:    probes,
	})
}

// service is everything the serving loop owns. Every dependency is an
// interface or a pure-Go type, so the shutdown sequence is testable without
// PostgreSQL or Kafka.
type service struct {
	config    *config.Config
	logger    *zap.Logger
	listener  net.Listener
	router    http.Handler
	store     repository.Store
	broker    events.Broker
	publisher *events.Publisher
	probes    *httpapi.ProbeState
}

// serve runs the outbox publisher and the HTTP server until ctx is cancelled,
// then flips readiness, drains in-flight requests and the outbox loop within the
// configured shutdown timeout, flushes the producer, and closes the pool.
func serve(ctx context.Context, svc service) error {
	server := &http.Server{
		Handler: svc.router,
		// Bound how long a silent client may hold a connection open before it
		// sends its request headers. No global read/write deadline: request
		// handling is bounded by the shutdown deadline instead.
		ReadHeaderTimeout: 10 * time.Second,
	}

	// The publisher loop outlives no request; it is cancelled explicitly during
	// shutdown so an in-flight batch is not abandoned mid-write.
	publisherCtx, stopPublisher := context.WithCancel(context.Background())
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		svc.publisher.Run(publisherCtx)
	}()

	serveErr := make(chan error, 1)
	go func() {
		// Shutdown and Close both surface as ErrServerClosed; neither is a fault.
		if err := server.Serve(svc.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	svc.logger.Info("http server listening",
		zap.String("service", serviceName),
		zap.String("trace_id", models.NewTraceID()),
		zap.String("operation", "startup"),
		zap.String("status", "ok"),
		zap.String("address", svc.listener.Addr().String()),
		zap.Int("port", svc.config.HTTPPort),
	)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-serveErr:
	}

	// Documented shutdown order: readiness first so no new traffic is routed
	// here, then in-flight requests and the outbox loop, then the producer
	// (which flushes), then the pool.
	svc.probes.BeginShutdown()
	svc.logger.Info("shutdown started",
		zap.String("service", serviceName),
		zap.String("trace_id", models.NewTraceID()),
		zap.String("operation", "shutdown"),
		zap.String("status", "ok"),
		zap.Duration("timeout", svc.config.ShutdownTimeout),
	)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), svc.config.ShutdownTimeout)
	defer cancelShutdown()

	if err := server.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("http shutdown: %w", err))
	}

	stopPublisher()
	select {
	case <-publisherDone:
	case <-shutdownCtx.Done():
		runErr = errors.Join(runErr, fmt.Errorf("outbox publisher did not drain within %s", svc.config.ShutdownTimeout))
	}

	if err := svc.broker.Close(); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("close kafka producer: %w", err))
	}
	svc.store.Close()

	svc.logger.Info("shutdown complete",
		zap.String("service", serviceName),
		zap.String("trace_id", models.NewTraceID()),
		zap.String("operation", "shutdown"),
		zap.String("status", "ok"),
	)
	return runErr
}

// listenAddress renders the TCP address for the configured port.
func listenAddress(port int) string {
	return net.JoinHostPort("", strconv.Itoa(port))
}

// newLogger builds the structured JSON logger from engineering guide §7. Logs go
// to out so tests can assert on the emitted lines.
func newLogger(level string, out io.Writer) (*zap.Logger, error) {
	parsed, err := zapcore.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("log level %q is not supported: %w", level, err)
	}
	if out == nil {
		out = io.Discard
	}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(out), parsed)
	return zap.New(core), nil
}

// logStartupFailure reports a rejected configuration and never leaks a value.
func logStartupFailure(logger *zap.Logger, err error) {
	logger.Error("configuration is invalid",
		zap.String("service", serviceName),
		zap.String("trace_id", models.NewTraceID()),
		zap.String("operation", "startup"),
		zap.String("status", "error"),
		zap.String("detail", err.Error()),
	)
	_ = logger.Sync()
}
