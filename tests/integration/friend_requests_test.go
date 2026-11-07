package integration_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"social_service/internal/models"
)

func TestFriendRequestEndpoints(t *testing.T) {
	t.Parallel()

	t.Run("send, list, accept", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		sender, recipient := uuid.New(), uuid.New()

		recorder := h.request(t, http.MethodPost, "/friend-requests",
			map[string]string{"recipient_user_id": recipient.String()}, h.tokenFor(sender), nil)
		require.Equal(t, http.StatusCreated, recorder.Code, "body: %s", recorder.Body.String())
		created := decode[models.FriendRequestDTO](t, recorder)
		assert.Equal(t, sender.String(), created.SenderUserID)
		assert.Equal(t, recipient.String(), created.RecipientUserID)
		assert.Equal(t, string(models.FriendRequestPending), created.Status)
		assert.False(t, created.CreatedAt.IsZero())

		// Incoming list for the recipient.
		recorder = h.request(t, http.MethodGet, "/friend-requests?direction=incoming", nil, h.tokenFor(recipient), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		page := decode[models.FriendRequestPage](t, recorder)
		require.Len(t, page.Data, 1)
		assert.Equal(t, created.RequestID, page.Data[0].RequestID)
		assert.Nil(t, page.NextCursor)

		// The default direction is incoming.
		recorder = h.request(t, http.MethodGet, "/friend-requests", nil, h.tokenFor(recipient), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		assert.Len(t, decode[models.FriendRequestPage](t, recorder).Data, 1)

		// Outgoing list for the sender.
		recorder = h.request(t, http.MethodGet, "/friend-requests?direction=outgoing", nil, h.tokenFor(sender), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		assert.Len(t, decode[models.FriendRequestPage](t, recorder).Data, 1)

		// Accept.
		recorder = h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/accept", nil, h.tokenFor(recipient), nil)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())
		friendship := decode[models.FriendshipDTO](t, recorder)
		assert.Equal(t, sender.String(), friendship.FriendUserID)

		// Friends are visible to both sides.
		for _, user := range []uuid.UUID{sender, recipient} {
			recorder = h.request(t, http.MethodGet, "/friends", nil, h.tokenFor(user), nil)
			require.Equal(t, http.StatusOK, recorder.Code)
			page := decode[models.FriendListPage](t, recorder)
			require.Len(t, page.Data, 1)
			assert.Nil(t, page.NextCursor)
		}
	})

	t.Run("send conflicts and validation", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		sender, recipient, other := uuid.New(), uuid.New(), uuid.New()
		h.becomeFriends(t, sender, recipient)

		recorder := h.request(t, http.MethodPost, "/friend-requests",
			map[string]string{"recipient_user_id": recipient.String()}, h.tokenFor(sender), nil)
		h.assertStatus(t, recorder, http.StatusConflict, models.CodeAlreadyFriends)

		recorder = h.request(t, http.MethodPost, "/friend-requests",
			map[string]string{"recipient_user_id": sender.String()}, h.tokenFor(sender), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeSelfRequest)

		require.Equal(t, http.StatusCreated, h.request(t, http.MethodPost, "/friend-requests",
			map[string]string{"recipient_user_id": other.String()}, h.tokenFor(sender), nil).Code)

		recorder = h.request(t, http.MethodPost, "/friend-requests",
			map[string]string{"recipient_user_id": other.String()}, h.tokenFor(sender), nil)
		h.assertStatus(t, recorder, http.StatusConflict, models.CodeRequestPending)

		// The reverse direction conflicts too.
		thirdParty := uuid.New()
		require.Equal(t, http.StatusCreated, h.request(t, http.MethodPost, "/friend-requests",
			map[string]string{"recipient_user_id": thirdParty.String()}, h.tokenFor(other), nil).Code)
		recorder = h.request(t, http.MethodPost, "/friend-requests",
			map[string]string{"recipient_user_id": other.String()}, h.tokenFor(thirdParty), nil)
		h.assertStatus(t, recorder, http.StatusConflict, models.CodeRequestPending)

		// Malformed bodies.
		recorder = h.request(t, http.MethodPost, "/friend-requests", []byte("{not-json"), h.tokenFor(sender), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)

		recorder = h.request(t, http.MethodPost, "/friend-requests",
			map[string]string{"recipient_user_id": "not-a-uuid"}, h.tokenFor(sender), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)
	})

	t.Run("accept, decline and cancel authorization", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		sender, recipient, outsider := uuid.New(), uuid.New(), uuid.New()
		created := h.sendFriendRequest(t, sender, recipient)

		recorder := h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/accept", nil, h.tokenFor(sender), nil)
		h.assertStatus(t, recorder, http.StatusNotFound, "")

		recorder = h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/accept", nil, h.tokenFor(outsider), nil)
		h.assertStatus(t, recorder, http.StatusNotFound, "")

		recorder = h.request(t, http.MethodDelete, "/friend-requests/"+created.RequestID, nil, h.tokenFor(recipient), nil)
		h.assertStatus(t, recorder, http.StatusNotFound, "")

		recorder = h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/decline", nil, h.tokenFor(sender), nil)
		h.assertStatus(t, recorder, http.StatusNotFound, "")

		recorder = h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/decline", nil, h.tokenFor(recipient), nil)
		require.Equal(t, http.StatusNoContent, recorder.Code)
		assert.Empty(t, recorder.Body.String())

		recorder = h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/decline", nil, h.tokenFor(recipient), nil)
		h.assertStatus(t, recorder, http.StatusConflict, models.CodeRequestNotPending)

		recorder = h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/accept", nil, h.tokenFor(recipient), nil)
		h.assertStatus(t, recorder, http.StatusConflict, models.CodeRequestNotPending)
	})

	t.Run("cancel then accept conflicts", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		sender, recipient := uuid.New(), uuid.New()
		created := h.sendFriendRequest(t, sender, recipient)

		recorder := h.request(t, http.MethodDelete, "/friend-requests/"+created.RequestID, nil, h.tokenFor(sender), nil)
		require.Equal(t, http.StatusNoContent, recorder.Code)

		recorder = h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/accept", nil, h.tokenFor(recipient), nil)
		h.assertStatus(t, recorder, http.StatusConflict, models.CodeRequestNotPending)
	})

	t.Run("repeat accept returns the same friendship", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		sender, recipient := uuid.New(), uuid.New()
		created := h.sendFriendRequest(t, sender, recipient)

		first := h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/accept", nil, h.tokenFor(recipient), nil)
		require.Equal(t, http.StatusOK, first.Code)
		second := h.request(t, http.MethodPost, "/friend-requests/"+created.RequestID+"/accept", nil, h.tokenFor(recipient), nil)
		require.Equal(t, http.StatusOK, second.Code)
		assert.Equal(t, decode[models.FriendshipDTO](t, first), decode[models.FriendshipDTO](t, second))
	})

	t.Run("idempotency key replay", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		sender, recipient := uuid.New(), uuid.New()
		body := map[string]string{"recipient_user_id": recipient.String()}
		headers := map[string]string{"Idempotency-Key": "retry-1"}

		first := h.request(t, http.MethodPost, "/friend-requests", body, h.tokenFor(sender), headers)
		require.Equal(t, http.StatusCreated, first.Code)
		second := h.request(t, http.MethodPost, "/friend-requests", body, h.tokenFor(sender), headers)
		require.Equal(t, http.StatusCreated, second.Code)
		assert.Equal(t, decode[models.FriendRequestDTO](t, first), decode[models.FriendRequestDTO](t, second))

		reused := h.request(t, http.MethodPost, "/friend-requests",
			map[string]string{"recipient_user_id": uuid.New().String()}, h.tokenFor(sender), headers)
		h.assertStatus(t, reused, http.StatusConflict, models.CodeIdempotencyKeyReuse)

		tooLong := make([]byte, models.MaxIdempotencyKeyLength+1)
		for i := range tooLong {
			tooLong[i] = 'k'
		}
		bad := h.request(t, http.MethodPost, "/friend-requests", map[string]string{"recipient_user_id": uuid.New().String()},
			h.tokenFor(sender), map[string]string{"Idempotency-Key": string(tooLong)})
		h.assertStatus(t, bad, http.StatusBadRequest, models.CodeInvalidRequest)
	})

	t.Run("pagination and validation of query parameters", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		caller := uuid.New()
		for i := 0; i < 3; i++ {
			h.sendFriendRequest(t, uuid.New(), caller)
		}

		seen := map[string]struct{}{}
		path := "/friend-requests?direction=incoming&limit=1"
		for page := 0; page < 3; page++ {
			recorder := h.request(t, http.MethodGet, path, nil, h.tokenFor(caller), nil)
			require.Equal(t, http.StatusOK, recorder.Code)
			body := decode[models.FriendRequestPage](t, recorder)
			require.Len(t, body.Data, 1)
			_, duplicate := seen[body.Data[0].RequestID]
			assert.False(t, duplicate, "pages must not duplicate rows")
			seen[body.Data[0].RequestID] = struct{}{}
			if page < 2 {
				require.NotNil(t, body.NextCursor, "page %d", page)
				path = "/friend-requests?direction=incoming&limit=1&cursor=" + *body.NextCursor
			} else {
				assert.Nil(t, body.NextCursor, "the last page has no next_cursor")
			}
		}
		assert.Len(t, seen, 3)

		for _, query := range []string{
			"direction=sideways",
			"limit=101",
			"limit=zero",
			"cursor=not-a-cursor",
		} {
			recorder := h.request(t, http.MethodGet, "/friend-requests?"+query, nil, h.tokenFor(caller), nil)
			h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)
		}
	})

	t.Run("remove friend", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		caller, friend := uuid.New(), uuid.New()
		h.becomeFriends(t, caller, friend)

		recorder := h.request(t, http.MethodDelete, "/friends/"+friend.String(), nil, h.tokenFor(caller), nil)
		require.Equal(t, http.StatusNoContent, recorder.Code)

		recorder = h.request(t, http.MethodDelete, "/friends/"+friend.String(), nil, h.tokenFor(caller), nil)
		h.assertStatus(t, recorder, http.StatusNotFound, models.CodeNotFriends)

		recorder = h.request(t, http.MethodDelete, "/friends/not-a-uuid", nil, h.tokenFor(caller), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)
	})

	t.Run("friend list pagination", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		caller := uuid.New()
		for i := 0; i < 2; i++ {
			h.becomeFriends(t, caller, uuid.New())
			h.clock.Advance(1)
		}

		recorder := h.request(t, http.MethodGet, "/friends?limit=1", nil, h.tokenFor(caller), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		page := decode[models.FriendListPage](t, recorder)
		require.Len(t, page.Data, 1)
		require.NotNil(t, page.NextCursor)

		recorder = h.request(t, http.MethodGet, fmt.Sprintf("/friends?limit=1&cursor=%s", *page.NextCursor), nil, h.tokenFor(caller), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		nextPage := decode[models.FriendListPage](t, recorder)
		require.Len(t, nextPage.Data, 1)
		assert.NotEqual(t, page.Data[0].FriendUserID, nextPage.Data[0].FriendUserID)
		assert.Nil(t, nextPage.NextCursor)
	})
}
