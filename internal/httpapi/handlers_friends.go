package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"social_service/internal/models"
)

// listFriends handles GET /friends.
func (a *API) listFriends(c *gin.Context) {
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
	friendships, next, listErr := a.deps.Friendship.ListFriends(c.Request.Context(), principal.UserID, cursor, limit)
	if listErr != nil {
		a.respondError(c, listErr)
		return
	}
	data := make([]models.FriendshipDTO, 0, len(friendships))
	for _, friendship := range friendships {
		dto, ok := models.NewFriendshipDTO(friendship, principal.UserID)
		if !ok {
			continue
		}
		data = append(data, dto)
	}
	c.JSON(http.StatusOK, models.FriendListPage{Data: data, NextCursor: next})
}

// removeFriend handles DELETE /friends/{user_id}.
func (a *API) removeFriend(c *gin.Context) {
	principal, ok := principalFrom(c)
	if !ok {
		a.respondError(c, models.UnauthorizedError("authentication required"))
		return
	}
	friendID, err := pathUUID(c, "user_id")
	if err != nil {
		a.respondError(c, err)
		return
	}
	if err := a.deps.Friendship.RemoveFriend(c.Request.Context(), principal.UserID, friendID); err != nil {
		a.respondError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
