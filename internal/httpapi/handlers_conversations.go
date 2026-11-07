package httpapi

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"social_service/internal/conversation"
	"social_service/internal/models"
)

type openDirectConversationRequest struct {
	FriendUserID string `json:"friend_user_id"`
}

type createGroupConversationRequest struct {
	Name           *string   `json:"name"`
	ParticipantIDs *[]string `json:"participant_ids"`
}

type addParticipantsRequest struct {
	UserIDs *[]string `json:"user_ids"`
}

type renameConversationRequest struct {
	Name *string `json:"name"`
}

// openDirectConversation handles POST /conversations/direct: 201 on creation,
// 200 with the existing conversation when the pair already has one.
func (a *API) openDirectConversation(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	var payload openDirectConversationRequest
	body, err := decodeBody(c, &payload)
	if err != nil {
		a.respondError(c, err)
		return
	}
	friendID, parseErr := uuid.Parse(payload.FriendUserID)
	if parseErr != nil {
		a.badRequest(c, "friend_user_id must be a uuid")
		return
	}
	key, keyErr := idempotencyKey(c, body)
	if keyErr != nil {
		a.respondError(c, keyErr)
		return
	}
	created, status, openErr := a.deps.Conversation.OpenDirect(c.Request.Context(), conversation.OpenDirectInput{
		CallerID:       principal.UserID,
		FriendID:       friendID,
		TraceID:        models.TraceIDFromContext(c.Request.Context()),
		IdempotencyKey: key,
	})
	if openErr != nil {
		a.respondError(c, openErr)
		return
	}
	c.JSON(status, models.NewConversationDTO(created))
}

// createGroupConversation handles POST /conversations/group.
func (a *API) createGroupConversation(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	var payload createGroupConversationRequest
	body, err := decodeBody(c, &payload)
	if err != nil {
		a.respondError(c, err)
		return
	}
	if payload.ParticipantIDs == nil {
		a.badRequest(c, "participant_ids is required")
		return
	}
	participantIDs, parseErr := parseUUIDList(*payload.ParticipantIDs)
	if parseErr != nil {
		a.respondError(c, parseErr)
		return
	}
	key, keyErr := idempotencyKey(c, body)
	if keyErr != nil {
		a.respondError(c, keyErr)
		return
	}
	created, createErr := a.deps.Conversation.CreateGroup(c.Request.Context(), conversation.CreateGroupInput{
		CallerID:       principal.UserID,
		Name:           payload.Name,
		ParticipantIDs: participantIDs,
		TraceID:        models.TraceIDFromContext(c.Request.Context()),
		IdempotencyKey: key,
	})
	if createErr != nil {
		a.respondError(c, createErr)
		return
	}
	c.JSON(http.StatusCreated, models.NewConversationDTO(created))
}

// listConversations handles GET /conversations.
func (a *API) listConversations(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
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
	conversations, next, listErr := a.deps.Conversation.List(c.Request.Context(), principal.UserID, cursor, limit)
	if listErr != nil {
		a.respondError(c, listErr)
		return
	}
	c.JSON(http.StatusOK, models.ConversationPage{
		Data:       models.NewConversationDTOs(conversations),
		NextCursor: next,
	})
}

// addConversationParticipants handles POST /conversations/{conversation_id}/participants.
func (a *API) addConversationParticipants(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	conversationID, err := pathUUID(c, "conversation_id")
	if err != nil {
		a.respondError(c, err)
		return
	}
	var payload addParticipantsRequest
	if _, bodyErr := decodeBody(c, &payload); bodyErr != nil {
		a.respondError(c, bodyErr)
		return
	}
	if payload.UserIDs == nil {
		a.badRequest(c, "user_ids is required")
		return
	}
	userIDs, parseErr := parseUUIDList(*payload.UserIDs)
	if parseErr != nil {
		a.respondError(c, parseErr)
		return
	}
	updated, addErr := a.deps.Conversation.AddParticipants(c.Request.Context(), principal.UserID, conversationID, userIDs, models.TraceIDFromContext(c.Request.Context()))
	if addErr != nil {
		a.respondError(c, addErr)
		return
	}
	c.JSON(http.StatusOK, models.NewConversationDTO(updated))
}

// leaveConversation handles POST /conversations/{conversation_id}/leave.
func (a *API) leaveConversation(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	conversationID, err := pathUUID(c, "conversation_id")
	if err != nil {
		a.respondError(c, err)
		return
	}
	if leaveErr := a.deps.Conversation.Leave(c.Request.Context(), principal.UserID, conversationID, models.TraceIDFromContext(c.Request.Context())); leaveErr != nil {
		a.respondError(c, leaveErr)
		return
	}
	c.Status(http.StatusNoContent)
}

// renameConversation handles PATCH /conversations/{conversation_id}.
func (a *API) renameConversation(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	conversationID, err := pathUUID(c, "conversation_id")
	if err != nil {
		a.respondError(c, err)
		return
	}
	var payload renameConversationRequest
	if _, bodyErr := decodeBody(c, &payload); bodyErr != nil {
		a.respondError(c, bodyErr)
		return
	}
	if payload.Name == nil {
		a.badRequest(c, "name is required")
		return
	}
	updated, renameErr := a.renameConversationName(c.Request.Context(), principal.UserID, conversationID, *payload.Name)
	if renameErr != nil {
		a.respondError(c, renameErr)
		return
	}
	c.JSON(http.StatusOK, models.NewConversationDTO(updated))
}

func (a *API) renameConversationName(ctx context.Context, callerID, conversationID uuid.UUID, name string) (models.Conversation, error) {
	return a.deps.Conversation.Rename(ctx, callerID, conversationID, name, models.TraceIDFromContext(ctx))
}
