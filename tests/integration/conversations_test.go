package integration_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"social_service/internal/models"
)

type openDirectBody struct {
	FriendUserID string `json:"friend_user_id"`
}

type createGroupBody struct {
	Name           *string  `json:"name"`
	ParticipantIDs []string `json:"participant_ids"`
}

type addParticipantsBody struct {
	UserIDs []string `json:"user_ids"`
}

func TestDirectConversationEndpoints(t *testing.T) {
	t.Parallel()

	t.Run("create then reuse", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		caller, friend := uuid.New(), uuid.New()
		h.becomeFriends(t, caller, friend)

		recorder := h.request(t, http.MethodPost, "/conversations/direct",
			openDirectBody{FriendUserID: friend.String()}, h.tokenFor(caller), nil)
		require.Equal(t, http.StatusCreated, recorder.Code, "body: %s", recorder.Body.String())
		created := decode[models.ConversationDTO](t, recorder)
		assert.Equal(t, string(models.ConversationDirect), created.Kind)
		assert.Nil(t, created.Name)
		assert.Equal(t, 1, created.ConversationVersion)
		assert.Len(t, created.Participants, 2)

		recorder = h.request(t, http.MethodPost, "/conversations/direct",
			openDirectBody{FriendUserID: caller.String()}, h.tokenFor(friend), nil)
		require.Equal(t, http.StatusOK, recorder.Code, "a reuse returns 200")
		reused := decode[models.ConversationDTO](t, recorder)
		assert.Equal(t, created.ConversationID, reused.ConversationID)

		// The conversation appears in both sidebars.
		recorder = h.request(t, http.MethodGet, "/conversations", nil, h.tokenFor(caller), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		page := decode[models.ConversationPage](t, recorder)
		require.Len(t, page.Data, 1)
		assert.Equal(t, created.ConversationID, page.Data[0].ConversationID)
	})

	t.Run("validation", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		caller, stranger := uuid.New(), uuid.New()

		recorder := h.request(t, http.MethodPost, "/conversations/direct",
			openDirectBody{FriendUserID: stranger.String()}, h.tokenFor(caller), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeNotFriends)

		recorder = h.request(t, http.MethodPost, "/conversations/direct",
			openDirectBody{FriendUserID: caller.String()}, h.tokenFor(caller), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeSelfRequest)

		recorder = h.request(t, http.MethodPost, "/conversations/direct",
			openDirectBody{FriendUserID: "nope"}, h.tokenFor(caller), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)
	})

	t.Run("idempotency key", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		caller, friend := uuid.New(), uuid.New()
		h.becomeFriends(t, caller, friend)
		body := openDirectBody{FriendUserID: friend.String()}
		headers := map[string]string{"Idempotency-Key": "dm-1"}

		first := h.request(t, http.MethodPost, "/conversations/direct", body, h.tokenFor(caller), headers)
		require.Equal(t, http.StatusCreated, first.Code)
		second := h.request(t, http.MethodPost, "/conversations/direct", body, h.tokenFor(caller), headers)
		require.Equal(t, http.StatusCreated, second.Code)
		assert.Equal(t, decode[models.ConversationDTO](t, first).ConversationID,
			decode[models.ConversationDTO](t, second).ConversationID)

		reused := h.request(t, http.MethodPost, "/conversations/direct",
			openDirectBody{FriendUserID: uuid.New().String()}, h.tokenFor(caller), headers)
		h.assertStatus(t, reused, http.StatusConflict, models.CodeIdempotencyKeyReuse)
	})
}

func TestGroupConversationEndpoints(t *testing.T) {
	t.Parallel()

	t.Run("create, add, rename, leave", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		creator, first, second := uuid.New(), uuid.New(), uuid.New()
		h.becomeFriends(t, creator, first)
		h.becomeFriends(t, creator, second)
		name := "Weekend Plans"

		recorder := h.request(t, http.MethodPost, "/conversations/group",
			createGroupBody{Name: &name, ParticipantIDs: []string{first.String()}}, h.tokenFor(creator), nil)
		require.Equal(t, http.StatusCreated, recorder.Code, "body: %s", recorder.Body.String())
		group := decode[models.ConversationDTO](t, recorder)
		assert.Equal(t, string(models.ConversationGroup), group.Kind)
		require.NotNil(t, group.Name)
		assert.Equal(t, name, *group.Name)
		assert.Equal(t, 1, group.ConversationVersion)

		// Add a participant: version bumps and the caller's friend is added.
		recorder = h.request(t, http.MethodPost, "/conversations/"+group.ConversationID+"/participants",
			addParticipantsBody{UserIDs: []string{second.String(), first.String()}}, h.tokenFor(creator), nil)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())
		added := decode[models.ConversationDTO](t, recorder)
		assert.Equal(t, 2, added.ConversationVersion)
		assert.Len(t, added.Participants, 3)

		// Rename.
		recorder = h.request(t, http.MethodPatch, "/conversations/"+group.ConversationID,
			map[string]string{"name": "Book Club"}, h.tokenFor(first), nil)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())
		renamed := decode[models.ConversationDTO](t, recorder)
		require.NotNil(t, renamed.Name)
		assert.Equal(t, "Book Club", *renamed.Name)
		assert.Equal(t, 3, renamed.ConversationVersion)

		// Leave.
		recorder = h.request(t, http.MethodPost, "/conversations/"+group.ConversationID+"/leave", nil, h.tokenFor(second), nil)
		require.Equal(t, http.StatusNoContent, recorder.Code)

		recorder = h.request(t, http.MethodGet, "/conversations", nil, h.tokenFor(second), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		assert.Empty(t, decode[models.ConversationPage](t, recorder).Data, "left conversations are excluded")

		recorder = h.request(t, http.MethodGet, "/conversations", nil, h.tokenFor(creator), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		assert.Len(t, decode[models.ConversationPage](t, recorder).Data, 1)
	})

	t.Run("group creation validation matrix", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		creator := uuid.New()
		friend := uuid.New()
		stranger := uuid.New()
		h.becomeFriends(t, creator, friend)

		recorder := h.request(t, http.MethodPost, "/conversations/group",
			createGroupBody{ParticipantIDs: []string{}}, h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeMissingParticipants)

		manyIDs := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			other := uuid.New()
			h.becomeFriends(t, creator, other)
			manyIDs = append(manyIDs, other.String())
		}
		recorder = h.request(t, http.MethodPost, "/conversations/group",
			createGroupBody{ParticipantIDs: manyIDs}, h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeGroupTooLarge)

		recorder = h.request(t, http.MethodPost, "/conversations/group",
			createGroupBody{ParticipantIDs: []string{friend.String(), stranger.String()}}, h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeNotFriends)

		blank := "   "
		recorder = h.request(t, http.MethodPost, "/conversations/group",
			createGroupBody{Name: &blank, ParticipantIDs: []string{friend.String()}}, h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeInvalidName)

		tooLong := strings.Repeat("n", 101)
		recorder = h.request(t, http.MethodPost, "/conversations/group",
			createGroupBody{Name: &tooLong, ParticipantIDs: []string{friend.String()}}, h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeInvalidName)

		recorder = h.request(t, http.MethodPost, "/conversations/group", []byte("{}"), h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)

		// Nothing was created.
		recorder = h.request(t, http.MethodGet, "/conversations", nil, h.tokenFor(creator), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		assert.Empty(t, decode[models.ConversationPage](t, recorder).Data)
	})

	t.Run("participant authorization and capacity", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		creator, friend, outsider := uuid.New(), uuid.New(), uuid.New()
		h.becomeFriends(t, creator, friend)

		recorder := h.request(t, http.MethodPost, "/conversations/group",
			createGroupBody{ParticipantIDs: []string{friend.String()}}, h.tokenFor(creator), nil)
		require.Equal(t, http.StatusCreated, recorder.Code)
		group := decode[models.ConversationDTO](t, recorder)

		recorder = h.request(t, http.MethodPost, "/conversations/"+group.ConversationID+"/participants",
			addParticipantsBody{UserIDs: []string{outsider.String()}}, h.tokenFor(outsider), nil)
		h.assertStatus(t, recorder, http.StatusForbidden, models.CodeNotAParticipant)

		recorder = h.request(t, http.MethodPost, "/conversations/"+group.ConversationID+"/participants",
			addParticipantsBody{UserIDs: []string{uuid.New().String()}}, h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeNotFriends)

		// Fill the group, then the next addition is rejected.
		extra := make([]string, 0, 8)
		for i := 0; i < 8; i++ {
			other := uuid.New()
			h.becomeFriends(t, creator, other)
			extra = append(extra, other.String())
		}
		recorder = h.request(t, http.MethodPost, "/conversations/"+group.ConversationID+"/participants",
			addParticipantsBody{UserIDs: extra}, h.tokenFor(creator), nil)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		overflow := uuid.New()
		h.becomeFriends(t, creator, overflow)
		recorder = h.request(t, http.MethodPost, "/conversations/"+group.ConversationID+"/participants",
			addParticipantsBody{UserIDs: []string{overflow.String()}}, h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeGroupFull)

		recorder = h.request(t, http.MethodPost, "/conversations/"+group.ConversationID+"/participants",
			addParticipantsBody{}, h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)
	})

	t.Run("group-only operations on a direct conversation", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		caller, friend, extra := uuid.New(), uuid.New(), uuid.New()
		h.becomeFriends(t, caller, friend, extra)

		recorder := h.request(t, http.MethodPost, "/conversations/direct",
			openDirectBody{FriendUserID: friend.String()}, h.tokenFor(caller), nil)
		require.Equal(t, http.StatusCreated, recorder.Code)
		direct := decode[models.ConversationDTO](t, recorder)

		recorder = h.request(t, http.MethodPost, "/conversations/"+direct.ConversationID+"/leave", nil, h.tokenFor(caller), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeGroupOnlyOperation)

		recorder = h.request(t, http.MethodPatch, "/conversations/"+direct.ConversationID,
			map[string]string{"name": "Nope"}, h.tokenFor(caller), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeGroupOnlyOperation)

		recorder = h.request(t, http.MethodPost, "/conversations/"+direct.ConversationID+"/participants",
			addParticipantsBody{UserIDs: []string{extra.String()}}, h.tokenFor(caller), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeGroupOnlyOperation)
	})

	t.Run("not found and invalid ids", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		user := uuid.New()
		missing := uuid.New().String()

		recorder := h.request(t, http.MethodPost, "/conversations/"+missing+"/leave", nil, h.tokenFor(user), nil)
		h.assertStatus(t, recorder, http.StatusNotFound, "")

		recorder = h.request(t, http.MethodPatch, "/conversations/"+missing,
			map[string]string{"name": "Ghost"}, h.tokenFor(user), nil)
		h.assertStatus(t, recorder, http.StatusNotFound, "")

		recorder = h.request(t, http.MethodPost, "/conversations/not-a-uuid/leave", nil, h.tokenFor(user), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)

		recorder = h.request(t, http.MethodPatch, "/conversations/"+missing, []byte("{}"), h.tokenFor(user), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)

		recorder = h.request(t, http.MethodPatch, "/conversations/not-a-uuid",
			map[string]string{"name": "Ghost"}, h.tokenFor(user), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)
	})

	t.Run("rename validation", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		creator, friend, outsider := uuid.New(), uuid.New(), uuid.New()
		h.becomeFriends(t, creator, friend)

		recorder := h.request(t, http.MethodPost, "/conversations/group",
			createGroupBody{ParticipantIDs: []string{friend.String()}}, h.tokenFor(creator), nil)
		require.Equal(t, http.StatusCreated, recorder.Code)
		group := decode[models.ConversationDTO](t, recorder)

		recorder = h.request(t, http.MethodPatch, "/conversations/"+group.ConversationID,
			map[string]string{"name": "Nope"}, h.tokenFor(outsider), nil)
		h.assertStatus(t, recorder, http.StatusForbidden, models.CodeNotAParticipant)

		recorder = h.request(t, http.MethodPatch, "/conversations/"+group.ConversationID,
			map[string]string{"name": "   "}, h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusUnprocessableEntity, models.CodeInvalidName)

		recorder = h.request(t, http.MethodPatch, "/conversations/"+group.ConversationID,
			[]byte("{}"), h.tokenFor(creator), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)
	})

	t.Run("group creation idempotency", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		creator, friend := uuid.New(), uuid.New()
		h.becomeFriends(t, creator, friend)
		body := createGroupBody{ParticipantIDs: []string{friend.String()}}
		headers := map[string]string{"Idempotency-Key": "group-1"}

		first := h.request(t, http.MethodPost, "/conversations/group", body, h.tokenFor(creator), headers)
		require.Equal(t, http.StatusCreated, first.Code)
		second := h.request(t, http.MethodPost, "/conversations/group", body, h.tokenFor(creator), headers)
		require.Equal(t, http.StatusCreated, second.Code)
		assert.Equal(t, decode[models.ConversationDTO](t, first).ConversationID,
			decode[models.ConversationDTO](t, second).ConversationID)
	})

	t.Run("conversation listing pagination", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		caller := uuid.New()
		for i := 0; i < 2; i++ {
			friend := uuid.New()
			h.becomeFriends(t, caller, friend)
			recorder := h.request(t, http.MethodPost, "/conversations/direct",
				openDirectBody{FriendUserID: friend.String()}, h.tokenFor(caller), nil)
			require.Equal(t, http.StatusCreated, recorder.Code)
			h.clock.Advance(1)
		}

		recorder := h.request(t, http.MethodGet, "/conversations?limit=1", nil, h.tokenFor(caller), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		page := decode[models.ConversationPage](t, recorder)
		require.Len(t, page.Data, 1)
		require.NotNil(t, page.NextCursor)

		recorder = h.request(t, http.MethodGet, "/conversations?limit=1&cursor="+*page.NextCursor, nil, h.tokenFor(caller), nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		nextPage := decode[models.ConversationPage](t, recorder)
		require.Len(t, nextPage.Data, 1)
		assert.NotEqual(t, page.Data[0].ConversationID, nextPage.Data[0].ConversationID)

		recorder = h.request(t, http.MethodGet, "/conversations?limit=101", nil, h.tokenFor(caller), nil)
		h.assertStatus(t, recorder, http.StatusBadRequest, models.CodeInvalidRequest)
	})
}
