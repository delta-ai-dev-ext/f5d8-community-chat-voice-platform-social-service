package models

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// Trace headers accepted at the edge.
const (
	// TraceparentHeader is the W3C trace context header.
	TraceparentHeader = "traceparent"
	// TraceIDHeader is the platform's simple trace-id header.
	TraceIDHeader = "X-Trace-Id"
)

// TraceIDLength is the length of a 32-hex-character trace id.
const TraceIDLength = 32

type traceIDContextKey struct{}

// WithTraceID stores the trace id on the context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDContextKey{}, traceID)
}

// TraceIDFromContext returns the trace id stored on the context, or "".
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if traceID, ok := ctx.Value(traceIDContextKey{}).(string); ok {
		return traceID
	}
	return ""
}

// ValidTraceID reports whether s is 32 lowercase hex characters and not all
// zeroes.
func ValidTraceID(s string) bool {
	if len(s) != TraceIDLength {
		return false
	}
	allZero := true
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
		if r != '0' {
			allZero = false
		}
	}
	return !allZero
}

// NewTraceID mints a fresh 32-hex trace id.
func NewTraceID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand failure is unrecoverable and never empty by contract;
		// fall back to a non-zero constant rather than emitting "".
		return "00000000000000000000000000000001"
	}
	return hex.EncodeToString(buf[:])
}

// ParseTraceparent extracts the trace id from a W3C traceparent header value
// (`version-traceid-spanid-flags`).
func ParseTraceparent(value string) (string, bool) {
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) != 4 {
		return "", false
	}
	if len(parts[0]) != 2 || parts[0] == "ff" {
		return "", false
	}
	if len(parts[2]) != 16 {
		return "", false
	}
	if !ValidTraceID(parts[1]) {
		return "", false
	}
	return parts[1], true
}

// ResolveTraceID picks the trace id from the accepted headers, minting one when
// none of them carries a valid value. The result is never empty.
func ResolveTraceID(get func(string) string) string {
	if value := get(TraceparentHeader); value != "" {
		if traceID, ok := ParseTraceparent(value); ok {
			return traceID
		}
	}
	if value := strings.ToLower(strings.TrimSpace(get(TraceIDHeader))); ValidTraceID(value) {
		return value
	}
	if value := strings.ToLower(strings.TrimSpace(get("Traceparent"))); value != "" {
		if traceID, ok := ParseTraceparent(value); ok {
			return traceID
		}
	}
	return NewTraceID()
}
