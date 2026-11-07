package httpapi

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"social_service/internal/friendship"
	"social_service/internal/models"
)

type sendFriendRequestRequest struct {
	RecipientUserID string `json:"recipient_user_id"`
}

// sendFriendRequest handles POST /friend-requests.
func (a *API) sendFriendRequest(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	var payload sendFriendRequestRequest
	body, err := decodeBody(c, &payload)
	if err != nil {
		a.respondError(c, err)
		return
	}
	recipientID, parseErr := uuid.Parse(payload.RecipientUserID)
	if parseErr != nil {
		a.badRequest(c, "recipient_user_id must be a uuid")
		return
	}
	key, keyErr := idempotencyKey(c, body)
	if keyErr != nil {
		a.respondError(c, keyErr)
		return
	}
	request, sendErr := a.deps.Friendship.Send(c.Request.Context(), friendship.SendInput{
		CallerID:       principal.UserID,
		RecipientID:    recipientID,
		TraceID:        models.TraceIDFromContext(c.Request.Context()),
		IdempotencyKey: key,
	})
	if sendErr != nil {
		a.respondError(c, sendErr)
		return
	}
	c.JSON(http.StatusCreated, models.NewFriendRequestDTO(request))
}

// listFriendRequests handles GET /friend-requests.
func (a *API) listFriendRequests(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	direction, validDirection := models.ParseRequestDirection(c.Query("direction"))
	if !validDirection {
		a.badRequest(c, "direction must be incoming or outgoing")
		return
	}
	cursor, err := cursorParam(c)
	if err != nil {
		a.respondError(c, err)
		return
	}
	limit, err := limitParam(c)
	if err != nil {
		a.respondError(c, err)
		return
	}
	requests, next, listErr := a.deps.Friendship.ListRequests(c.Request.Context(), principal.UserID, direction, cursor, limit)
	if listErr != nil {
		a.respondError(c, listErr)
		return
	}
	c.JSON(http.StatusOK, models.FriendRequestPage{
		Data:       models.NewFriendRequestDTOs(requests),
		NextCursor: next,
	})
}

// acceptFriendRequest handles POST /friend-requests/{request_id}/accept.
func (a *API) acceptFriendRequest(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	requestID, err := pathUUID(c, "request_id")
	if err != nil {
		a.respondError(c, err)
		return
	}
	friendshipRow, acceptErr := a.deps.Friendship.Accept(c.Request.Context(), principal.UserID, requestID, models.TraceIDFromContext(c.Request.Context()))
	if acceptErr != nil {
		a.respondError(c, acceptErr)
		return
	}
	dto, ok := models.NewFriendshipDTO(friendshipRow, principal.UserID)
	if !ok {
		a.respondError(c, models.ErrFriendRequestNotFound())
		return
	}
	c.JSON(http.StatusOK, dto)
}

// declineFriendRequest handles POST /friend-requests/{request_id}/decline.
func (a *API) declineFriendRequest(c *gin.Context) {
	a.resolveFriendRequest(c, a.deps.Friendship.Decline)
}

// cancelFriendRequest handles DELETE /friend-requests/{request_id}.
func (a *API) cancelFriendRequest(c *gin.Context) {
	a.resolveFriendRequest(c, a.deps.Friendship.Cancel)
}

func (a *API) resolveFriendRequest(c *gin.Context, resolve func(ctx context.Context, callerID, requestID uuid.UUID, traceID string) error) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	requestID, err := pathUUID(c, "request_id")
	if err != nil {
		a.respondError(c, err)
		return
	}
	if err := resolve(c.Request.Context(), principal.UserID, requestID, models.TraceIDFromContext(c.Request.Context())); err != nil {
		a.respondError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
