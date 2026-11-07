package httpapi

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"social_service/internal/models"
)

const principalContextKey = "social_service.principal"

// principalFrom returns the authenticated caller stored by the auth middleware.
func principalFrom(c *gin.Context) (models.Principal, bool) {
	value, ok := c.Get(principalContextKey)
	if !ok {
		return models.Principal{}, false
	}
	principal, ok := value.(models.Principal)
	return principal, ok
}

// traceMiddleware resolves the trace id (traceparent / X-Trace-Id, minted when
// absent) and publishes it on the context and response headers.
func (a *API) traceMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := models.ResolveTraceID(c.GetHeader)
		c.Writer.Header().Set(models.TraceIDHeader, traceID)
		c.Request = c.Request.WithContext(models.WithTraceID(c.Request.Context(), traceID))
		c.Next()
	}
}

// observeMiddleware records RED metrics and one structured log line per
// request. Log fields never include user ids or names.
func (a *API) observeMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := a.clock.Now()
		c.Next()
		elapsed := a.clock.Now().Sub(started)
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		status := c.Writer.Status()
		a.deps.Metrics.Observe(route, c.Request.Method, status, elapsed.Seconds())
		a.deps.Logger.Info("http request",
			zap.String("service", "social_service"),
			zap.String("trace_id", models.TraceIDFromContext(c.Request.Context())),
			zap.String("operation", c.Request.Method+" "+route),
			zap.Int("status", status),
			zap.Float64("duration_seconds", elapsed.Seconds()),
		)
	}
}

// recoveryMiddleware converts a panic into a 500 without leaking internals.
func (a *API) recoveryMiddleware() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		a.deps.Logger.Error("panic recovered",
			zap.String("service", "social_service"),
			zap.String("trace_id", models.TraceIDFromContext(c.Request.Context())),
			zap.String("operation", c.Request.Method+" "+c.FullPath()),
			zap.String("status", "panic"),
		)
		c.AbortWithStatusJSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:  string(models.CodeInternal),
			Detail: "internal error",
		})
	})
}

// authMiddleware verifies the bearer token and scopes the request to its `sub`.
func (a *API) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		rawToken, err := bearerToken(c.GetHeader("Authorization"))
		if err != nil {
			a.respondError(c, models.UnauthorizedError("missing or malformed Authorization header"))
			return
		}
		principal, err := a.deps.Verifier.Verify(c.Request.Context(), rawToken)
		if err != nil {
			a.respondError(c, models.UnauthorizedError("access token is invalid or expired"))
			return
		}
		c.Set(principalContextKey, principal)
		c.Next()
	}
}

// healthz is the liveness probe: process responsiveness only, never PostgreSQL
// or Kafka.
func (a *API) healthz(c *gin.Context) {
	if err := a.deps.Probes.Liveness(); err != nil {
		c.JSON(http.StatusServiceUnavailable, models.ErrorResponse{
			Error:  string(models.CodeInternal),
			Detail: "shutting down",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// readyz is the readiness probe: PostgreSQL pool + Kafka producer, and 503 once
// shutdown has started.
func (a *API) readyz(c *gin.Context) {
	if err := a.deps.Probes.Readiness(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, models.ErrorResponse{
			Error:  string(models.CodeInternal),
			Detail: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// ProbeState tracks dependency and shutdown health for the probes.
type ProbeState struct {
	shuttingDown bool
	ping         func(ctx context.Context) error
	kafkaHealthy func() bool
}

// NewProbeState builds a probe state from dependency checks.
func NewProbeState(ping func(ctx context.Context) error, kafkaHealthy func() bool) *ProbeState {
	return &ProbeState{ping: ping, kafkaHealthy: kafkaHealthy}
}

// BeginShutdown flips readiness to failing. Subsequent readiness checks return
// an error until the process exits.
func (p *ProbeState) BeginShutdown() {
	if p != nil {
		p.shuttingDown = true
	}
}

// ShuttingDown reports whether the shutdown sequence has started.
func (p *ProbeState) ShuttingDown() bool {
	return p != nil && p.shuttingDown
}

// Liveness fails only during shutdown.
func (p *ProbeState) Liveness() error {
	if p.ShuttingDown() {
		return errShuttingDown
	}
	return nil
}

// Readiness checks PostgreSQL and Kafka.
func (p *ProbeState) Readiness(ctx context.Context) error {
	if p.ShuttingDown() {
		return errShuttingDown
	}
	if p.ping != nil {
		if err := p.ping(ctx); err != nil {
			return errDependencyUnavailable
		}
	}
	if p.kafkaHealthy != nil && !p.kafkaHealthy() {
		return errDependencyUnavailable
	}
	return nil
}
