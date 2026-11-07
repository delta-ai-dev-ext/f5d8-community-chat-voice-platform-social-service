package unit_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"social_service/internal/models"
)

func TestErrorCodesMapToDocumentedStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		err    *models.DomainError
		code   models.ErrorCode
		status int
	}{
		{"self request", models.ErrSelfRequest(), models.CodeSelfRequest, http.StatusUnprocessableEntity},
		{"already friends", models.ErrAlreadyFriends(), models.CodeAlreadyFriends, http.StatusConflict},
		{"request pending", models.ErrRequestPending(), models.CodeRequestPending, http.StatusConflict},
		{"request not pending", models.ErrRequestNotPending(), models.CodeRequestNotPending, http.StatusConflict},
		{"not friends (validation)", models.ErrNotFriendsValidation(), models.CodeNotFriends, http.StatusUnprocessableEntity},
		{"not friends (removal)", models.ErrNotFriendsNotFound(), models.CodeNotFriends, http.StatusNotFound},
		{"group too large", models.ErrGroupTooLarge(), models.CodeGroupTooLarge, http.StatusUnprocessableEntity},
		{"group full", models.ErrGroupFull(), models.CodeGroupFull, http.StatusUnprocessableEntity},
		{"missing participants", models.ErrMissingParticipants(), models.CodeMissingParticipants, http.StatusUnprocessableEntity},
		{"not a participant", models.ErrNotAParticipant(), models.CodeNotAParticipant, http.StatusForbidden},
		{"group only operation", models.ErrGroupOnlyOperation(), models.CodeGroupOnlyOperation, http.StatusUnprocessableEntity},
		{"invalid name", models.ErrInvalidName(), models.CodeInvalidName, http.StatusUnprocessableEntity},
		{"idempotency key reuse", models.ErrIdempotencyKeyReuse(), models.CodeIdempotencyKeyReuse, http.StatusConflict},
		{"unauthorized", models.UnauthorizedError("nope"), models.CodeUnauthorized, http.StatusUnauthorized},
		{"conversation not found", models.ErrConversationNotFound(), models.CodeNotFound, http.StatusNotFound},
		{"friend request not found", models.ErrFriendRequestNotFound(), models.CodeNotFound, http.StatusNotFound},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.code, test.err.Code)
			assert.Equal(t, test.status, test.err.Status)
			assert.NotEmpty(t, test.err.Detail)
			assert.Contains(t, test.err.Error(), string(test.code))
			assert.False(t, strings.Contains(test.err.Detail, "SELECT"), "detail must not leak internals")

			extracted, ok := models.AsDomainError(test.err)
			require.True(t, ok)
			assert.Same(t, test.err, extracted)
		})
	}
}

func TestAsDomainErrorWraps(t *testing.T) {
	t.Parallel()

	wrapped := errors.Join(errors.New("boom"), models.ErrGroupFull())
	domainErr, ok := models.AsDomainError(wrapped)
	require.True(t, ok)
	assert.Equal(t, models.CodeGroupFull, domainErr.Code)

	_, ok = models.AsDomainError(errors.New("plain"))
	assert.False(t, ok)
	_, ok = models.AsDomainError(nil)
	assert.False(t, ok)
}

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()

	cursor := models.Cursor{CreatedAt: time.Date(2026, 7, 30, 12, 0, 0, 123456789, time.UTC), ID: uuid.New()}
	decoded, ok, err := models.ParseCursor(cursor.Encode())
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, cursor.CreatedAt.Equal(decoded.CreatedAt))
	assert.Equal(t, cursor.ID, decoded.ID)

	_, ok, err = models.ParseCursor("")
	require.NoError(t, err)
	assert.False(t, ok, "an absent cursor means the first page")

	_, _, err = models.ParseCursor("not-a-cursor")
	assert.ErrorIs(t, err, models.ErrInvalidCursor)

	_, _, err = models.ParseCursor("bm90LWEtY3Vyc29y")
	assert.ErrorIs(t, err, models.ErrInvalidCursor)
}

func TestResolveLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		requested int
		limit     int
		ok        bool
	}{
		{0, models.DefaultPageLimit, true},
		{1, 1, true},
		{100, 100, true},
		{101, 0, false},
		{-1, 0, false},
	}

	for _, test := range tests {
		limit, ok := models.ResolveLimit(test.requested)
		assert.Equal(t, test.ok, ok, "requested=%d", test.requested)
		assert.Equal(t, test.limit, limit, "requested=%d", test.requested)
	}
}

func TestTraceIDHelpers(t *testing.T) {
	t.Parallel()

	assert.True(t, models.ValidTraceID("0123456789abcdef0123456789abcdef"))
	assert.False(t, models.ValidTraceID(""))
	assert.False(t, models.ValidTraceID("0123456789ABCDEF0123456789ABCDEF"), "trace ids are lowercase")
	assert.False(t, models.ValidTraceID("00000000000000000000000000000000"), "all-zeroes is invalid")
	assert.False(t, models.ValidTraceID("deadbeef"))

	minted := models.NewTraceID()
	assert.True(t, models.ValidTraceID(minted))

	traceID, ok := models.ParseTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	require.True(t, ok)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", traceID)

	_, ok = models.ParseTraceparent("garbage")
	assert.False(t, ok)

	headers := map[string]string{
		models.TraceparentHeader: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		models.TraceIDHeader:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", models.ResolveTraceID(func(name string) string { return headers[name] }))

	onlyTraceHeader := map[string]string{models.TraceIDHeader: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
	assert.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", models.ResolveTraceID(func(name string) string { return onlyTraceHeader[name] }))

	mintedEdge := models.ResolveTraceID(func(string) string { return "" })
	assert.True(t, models.ValidTraceID(mintedEdge), "a trace id is minted at the edge when absent")

	ctx := models.WithTraceID(t.Context(), "abcabcabcabcabcabcabcabcabcabcab")
	assert.Equal(t, "abcabcabcabcabcabcabcabcabcabcab", models.TraceIDFromContext(ctx))
	assert.Empty(t, models.TraceIDFromContext(t.Context()))
}

func TestCanonicalPairAndDTOMapping(t *testing.T) {
	t.Parallel()

	first := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	second := uuid.MustParse("22222222-2222-4222-8222-222222222222")

	lesser, greater := models.CanonicalPair(second, first)
	assert.Equal(t, first, lesser)
	assert.Equal(t, second, greater)
	reorderedLesser, reorderedGreater := models.CanonicalPair(first, second)
	assert.Equal(t, first, reorderedLesser, "canonical order is stable")
	assert.Equal(t, second, reorderedGreater)
	assert.Equal(t, first.String()+"|"+second.String(), models.PairKey(second, first))

	createdAt := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	name := "Team"
	conversation := models.Conversation{
		ID:        uuid.New(),
		Kind:      models.ConversationGroup,
		Name:      &name,
		CreatedBy: first,
		Version:   3,
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
		Participants: []models.Participant{
			{UserID: first, JoinedAt: createdAt},
			{UserID: second, JoinedAt: createdAt, LeftAt: &createdAt},
		},
	}
	dto := models.NewConversationDTO(conversation)
	assert.Equal(t, 3, dto.ConversationVersion)
	require.Len(t, dto.Participants, 1, "only current participants are returned")
	assert.Equal(t, first.String(), dto.Participants[0].UserID)
	require.NotNil(t, dto.Name)
	assert.Equal(t, "Team", *dto.Name)

	assert.Empty(t, models.NewConversationDTOs(nil), "pages render [] instead of null")
	assert.Empty(t, models.NewFriendRequestDTOs(nil))

	friendship := models.Friendship{UserIDA: first, UserIDB: second, CreatedAt: createdAt}
	friendshipDTO, ok := models.NewFriendshipDTO(friendship, second)
	require.True(t, ok)
	assert.Equal(t, first.String(), friendshipDTO.FriendUserID)

	_, ok = models.NewFriendshipDTO(friendship, uuid.New())
	assert.False(t, ok)
}

func TestRequestDirectionParsing(t *testing.T) {
	t.Parallel()

	direction, ok := models.ParseRequestDirection("")
	assert.True(t, ok)
	assert.Equal(t, models.DirectionIncoming, direction, "incoming is the default")

	direction, ok = models.ParseRequestDirection("outgoing")
	assert.True(t, ok)
	assert.Equal(t, models.DirectionOutgoing, direction)

	_, ok = models.ParseRequestDirection("sideways")
	assert.False(t, ok)
}

func TestIdempotencyKeyHashing(t *testing.T) {
	t.Parallel()

	assert.Nil(t, models.NewIdempotencyKey("", []byte("{}")))

	first := models.NewIdempotencyKey("key", []byte(`{"a":1}`))
	second := models.NewIdempotencyKey("key", []byte(`{"a":1}`))
	third := models.NewIdempotencyKey("key", []byte(`{"a":2}`))
	require.NotNil(t, first)
	assert.Equal(t, first.RequestHash, second.RequestHash)
	assert.NotEqual(t, first.RequestHash, third.RequestHash)
	assert.Equal(t, models.MaxIdempotencyKeyLength, 64)
}
