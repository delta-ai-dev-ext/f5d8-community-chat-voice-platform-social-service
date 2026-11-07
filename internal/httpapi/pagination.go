package httpapi

import (
	"encoding/json"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"social_service/internal/models"
)

// IdempotencyKeyHeader is the documented retry key header.
const IdempotencyKeyHeader = "Idempotency-Key"

// decodeBody reads the raw body once so the same bytes can be hashed for
// idempotency and decoded into the request struct.
func decodeBody(c *gin.Context, target any) ([]byte, error) {
	body, err := c.GetRawData()
	if err != nil {
		return nil, invalidRequest("request body could not be read")
	}
	if len(body) == 0 {
		return nil, invalidRequest("request body is required")
	}
	if err := json.Unmarshal(body, target); err != nil {
		return nil, invalidRequest("request body is not valid JSON")
	}
	return body, nil
}

// idempotencyKey reads the Idempotency-Key header and hashes the request body.
func idempotencyKey(c *gin.Context, body []byte) (*models.IdempotencyKey, error) {
	raw := c.GetHeader(IdempotencyKeyHeader)
	if raw == "" {
		return nil, nil
	}
	if len(raw) > models.MaxIdempotencyKeyLength {
		return nil, invalidRequest("Idempotency-Key must be at most 64 characters")
	}
	return models.NewIdempotencyKey(raw, body), nil
}

// cursorParam parses the opaque cursor query parameter.
func cursorParam(c *gin.Context) (*models.Cursor, error) {
	cursor, ok, err := models.ParseCursor(c.Query("cursor"))
	if err != nil {
		return nil, invalidRequest("cursor is not valid")
	}
	if !ok {
		return nil, nil
	}
	return &cursor, nil
}

// limitParam parses the limit query parameter (default 50, max 100).
func limitParam(c *gin.Context) (int, error) {
	raw := c.Query("limit")
	if raw == "" {
		return models.DefaultPageLimit, nil
	}
	requested, err := strconv.Atoi(raw)
	if err != nil {
		return 0, invalidRequest("limit must be an integer")
	}
	limit, ok := models.ResolveLimit(requested)
	if !ok {
		return 0, invalidRequest("limit must be between 1 and 100")
	}
	return limit, nil
}

// pathUUID parses a uuid path parameter.
func pathUUID(c *gin.Context, name string) (uuid.UUID, error) {
	value, err := uuid.Parse(c.Param(name))
	if err != nil {
		return uuid.Nil, invalidRequest(name + " must be a uuid")
	}
	return value, nil
}

// parseUUIDList converts the JSON string ids into uuids.
func parseUUIDList(values []string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, 0, len(values))
	for _, raw := range values {
		id, err := uuid.Parse(raw)
		if err != nil {
			return nil, invalidRequest("user ids must be uuids")
		}
		out = append(out, id)
	}
	return out, nil
}
