package unit_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"social_service/internal/conversation"
	"social_service/internal/events"
	"social_service/internal/models"
	"social_service/internal/repository"
	"social_service/tests/mocks"
)

type conversationFixture struct {
	store   *repository.MemoryStore
	clock   *mocks.FixedClock
	service *conversation.Service
}

func newConversationFixture(t *testing.T) *conversationFixture {
	t.Helper()
	store := repository.NewMemoryStore()
	clock := mocks.NewFixedClock(baseTime)
	metrics := conversation.NewMetrics(prometheus.NewRegistry())
	service := conversation.NewService(store, clock, zap.NewNop(), metrics)
	t.Cleanup(store.Close)
	return &conversationFixture{store: store, clock: clock, service: service}
}

func (f *conversationFixture) outbox(t *testing.T) []models.OutboxRecord {
	t.Helper()
	var records []models.OutboxRecord
	require.NoError(t, f.store.InTx(context.Background(), func(ctx context.Context, tx repository.Tx) error {
		fetched, err := tx.ListUnpublishedOutbox(ctx, 0)
		if err != nil {
			return err
		}
		records = fetched
		return nil
	}))
	return records
}

func (f *conversationFixture) becomeFriends(t *testing.T, first, second uuid.UUID) {
	t.Helper()
	lesser, greater := models.CanonicalPair(first, second)
	require.NoError(t, f.store.InTx(context.Background(), func(ctx context.Context, tx repository.Tx) error {
		return tx.InsertFriendship(ctx, models.Friendship{UserIDA: lesser, UserIDB: greater, CreatedAt: f.clock.Now()})
	}))
}

func (f *conversationFixture) makeFriends(t *testing.T, caller uuid.UUID, others ...uuid.UUID) {
	t.Helper()
	for _, other := range others {
		f.becomeFriends(t, caller, other)
	}
}

func decodeConversationPayload(t *testing.T, record models.OutboxRecord) events.ConversationPayload {
	t.Helper()
	var payload events.ConversationPayload
	require.NoError(t, json.Unmarshal(record.Payload, &payload))
	return payload
}

// conversationEvent selects the single outbox event carrying the given change.
// Rows sharing a created_at are ordered by id, so tests select by content.
func conversationEvent(t *testing.T, records []models.OutboxRecord, change string) models.OutboxRecord {
	t.Helper()
	var matches []models.OutboxRecord
	for _, record := range records {
		if decodeConversationPayload(t, record).Change == change {
			matches = append(matches, record)
		}
	}
	require.Len(t, matches, 1, "expected exactly one %s event", change)
	return matches[0]
}

func TestOpenDirectConversationCreatesThenReuses(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, friend := uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, friend)

	created, status, err := fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{
		CallerID: caller, FriendID: friend, TraceID: "0123456789abcdef0123456789abcdef",
	})
	require.NoError(t, err)
	assert.Equal(t, 201, status)
	assert.Equal(t, models.ConversationDirect, created.Kind)
	assert.Nil(t, created.Name, "direct conversations are nameless")
	assert.Equal(t, 1, created.Version)
	assert.ElementsMatch(t, []uuid.UUID{caller, friend}, created.ActiveParticipantIDs())

	records := fixture.outbox(t)
	require.Len(t, records, 1)
	record := records[0]
	assert.Equal(t, events.EventTypeConversationChanged, record.EventType)
	assert.Equal(t, created.ID.String(), record.PartitionKey, "EVT-008 is keyed by conversation_id")
	assert.Equal(t, events.CorrelationFor(created.ID), record.CorrelationID)
	assert.Nil(t, record.CausationID)

	payload := decodeConversationPayload(t, record)
	assert.Equal(t, events.ChangeCreated, payload.Change)
	assert.Equal(t, string(models.ConversationDirect), payload.Kind)
	assert.Equal(t, created.ID.String(), payload.ConversationID)
	assert.ElementsMatch(t, []string{caller.String(), friend.String()}, payload.ParticipantUserIDs)
	assert.Empty(t, payload.AddedUserIDs)
	assert.Empty(t, payload.RemovedUserIDs)
	assert.Equal(t, 1, payload.ConversationVersion)

	// Reuse: same conversation, 200, and no new event.
	reused, reuseStatus, err := fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{CallerID: friend, FriendID: caller})
	require.NoError(t, err)
	assert.Equal(t, 200, reuseStatus)
	assert.Equal(t, created.ID, reused.ID)
	assert.Len(t, fixture.outbox(t), 1, "a reuse publishes no event")
}

func TestOpenDirectConversationRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     func(caller, friend uuid.UUID) uuid.UUID
		makeFriend bool
		wantCode   models.ErrorCode
		wantStatus int
	}{
		{
			name:       "self request",
			target:     func(caller, _ uuid.UUID) uuid.UUID { return caller },
			makeFriend: true,
			wantCode:   models.CodeSelfRequest,
			wantStatus: 422,
		},
		{
			name:       "not friends",
			wantCode:   models.CodeNotFriends,
			wantStatus: 422,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := newConversationFixture(t)
			caller, friend := uuid.New(), uuid.New()
			if test.makeFriend {
				fixture.makeFriends(t, caller, friend)
			}
			target := friend
			if test.target != nil {
				target = test.target(caller, friend)
			}

			_, status, err := fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{CallerID: caller, FriendID: target})
			require.Error(t, err)
			assert.Zero(t, status)
			domainErr, ok := models.AsDomainError(err)
			require.True(t, ok)
			assert.Equal(t, test.wantCode, domainErr.Code)
			assert.Equal(t, test.wantStatus, domainErr.Status)
			assert.Empty(t, fixture.outbox(t), "nothing may be created")
		})
	}
}

func TestOpenDirectConversationIdempotency(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, friend := uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, friend)
	body := []byte(`{"friend_user_id":"` + friend.String() + `"}`)

	created, status, err := fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{
		CallerID: caller, FriendID: friend, IdempotencyKey: models.NewIdempotencyKey("direct-1", body),
	})
	require.NoError(t, err)
	require.Equal(t, 201, status)

	replayed, replayStatus, err := fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{
		CallerID: caller, FriendID: friend, IdempotencyKey: models.NewIdempotencyKey("direct-1", body),
	})
	require.NoError(t, err)
	assert.Equal(t, 201, replayStatus, "a replay returns the original status")
	assert.Equal(t, created.ID, replayed.ID)
	assert.Len(t, fixture.outbox(t), 1)

	otherBody := []byte(`{"friend_user_id":"` + uuid.New().String() + `"}`)
	_, _, err = fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{
		CallerID: caller, FriendID: friend, IdempotencyKey: models.NewIdempotencyKey("direct-1", otherBody),
	})
	require.Error(t, err)
	domainErr, ok := models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, models.CodeIdempotencyKeyReuse, domainErr.Code)
}

func TestCreateGroupConversation(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, first, second := uuid.New(), uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, first, second)
	name := "  Weekend Plans  "

	created, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
		CallerID:       caller,
		Name:           &name,
		ParticipantIDs: []uuid.UUID{first, second, first},
		TraceID:        "0123456789abcdef0123456789abcdef",
	})
	require.NoError(t, err)

	assert.Equal(t, models.ConversationGroup, created.Kind)
	require.NotNil(t, created.Name)
	assert.Equal(t, "Weekend Plans", *created.Name, "names are trimmed")
	assert.Equal(t, 1, created.Version)
	assert.ElementsMatch(t, []uuid.UUID{caller, first, second}, created.ActiveParticipantIDs(), "duplicates are ignored")

	records := fixture.outbox(t)
	require.Len(t, records, 1)
	payload := decodeConversationPayload(t, records[0])
	assert.Equal(t, events.ChangeCreated, payload.Change)
	assert.Equal(t, string(models.ConversationGroup), payload.Kind)
	assert.Len(t, payload.ParticipantUserIDs, 3)
	require.NotNil(t, payload.Name)
	assert.Equal(t, "Weekend Plans", *payload.Name)
}

func TestCreateGroupConversationRejections(t *testing.T) {
	t.Parallel()

	longName := ""
	for i := 0; i < conversation.MaxNameLength+1; i++ {
		longName += "n"
	}
	blank := "   "

	tests := []struct {
		name     string
		prepare  func(t *testing.T, fixture *conversationFixture, caller uuid.UUID) conversation.CreateGroupInput
		wantCode models.ErrorCode
	}{
		{
			name: "no participants",
			prepare: func(_ *testing.T, _ *conversationFixture, caller uuid.UUID) conversation.CreateGroupInput {
				return conversation.CreateGroupInput{CallerID: caller, ParticipantIDs: []uuid.UUID{}}
			},
			wantCode: models.CodeMissingParticipants,
		},
		{
			name: "only the caller",
			prepare: func(_ *testing.T, _ *conversationFixture, caller uuid.UUID) conversation.CreateGroupInput {
				return conversation.CreateGroupInput{CallerID: caller, ParticipantIDs: []uuid.UUID{caller}}
			},
			wantCode: models.CodeMissingParticipants,
		},
		{
			name: "ten friends is too large",
			prepare: func(t *testing.T, fixture *conversationFixture, caller uuid.UUID) conversation.CreateGroupInput {
				t.Helper()
				ids := make([]uuid.UUID, 0, conversation.MaxGroupFriends+1)
				for i := 0; i < conversation.MaxGroupFriends+1; i++ {
					friend := uuid.New()
					fixture.becomeFriends(t, caller, friend)
					ids = append(ids, friend)
				}
				return conversation.CreateGroupInput{CallerID: caller, ParticipantIDs: ids}
			},
			wantCode: models.CodeGroupTooLarge,
		},
		{
			name: "one participant is not a friend",
			prepare: func(t *testing.T, fixture *conversationFixture, caller uuid.UUID) conversation.CreateGroupInput {
				t.Helper()
				friend := uuid.New()
				fixture.becomeFriends(t, caller, friend)
				return conversation.CreateGroupInput{CallerID: caller, ParticipantIDs: []uuid.UUID{friend, uuid.New()}}
			},
			wantCode: models.CodeNotFriends,
		},
		{
			name: "blank name",
			prepare: func(t *testing.T, fixture *conversationFixture, caller uuid.UUID) conversation.CreateGroupInput {
				t.Helper()
				friend := uuid.New()
				fixture.becomeFriends(t, caller, friend)
				return conversation.CreateGroupInput{CallerID: caller, Name: &blank, ParticipantIDs: []uuid.UUID{friend}}
			},
			wantCode: models.CodeInvalidName,
		},
		{
			name: "name too long",
			prepare: func(t *testing.T, fixture *conversationFixture, caller uuid.UUID) conversation.CreateGroupInput {
				t.Helper()
				friend := uuid.New()
				fixture.becomeFriends(t, caller, friend)
				return conversation.CreateGroupInput{CallerID: caller, Name: &longName, ParticipantIDs: []uuid.UUID{friend}}
			},
			wantCode: models.CodeInvalidName,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := newConversationFixture(t)
			caller := uuid.New()
			input := test.prepare(t, fixture, caller)

			_, err := fixture.service.CreateGroup(context.Background(), input)
			require.Error(t, err)
			domainErr, ok := models.AsDomainError(err)
			require.True(t, ok)
			assert.Equal(t, test.wantCode, domainErr.Code)
			assert.Equal(t, 422, domainErr.Status)
			assert.Empty(t, fixture.outbox(t), "nothing may be created")

			conversations, _, listErr := fixture.service.List(context.Background(), caller, nil, models.MaxPageLimit)
			require.NoError(t, listErr)
			assert.Empty(t, conversations)
		})
	}
}

func TestAddParticipants(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, first, second := uuid.New(), uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, first, second)

	group, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
		CallerID: caller, ParticipantIDs: []uuid.UUID{first},
	})
	require.NoError(t, err)

	updated, err := fixture.service.AddParticipants(context.Background(), caller, group.ID, []uuid.UUID{second, second, first}, "ffffffffffffffffffffffffffffffff")
	require.NoError(t, err)
	assert.Equal(t, 2, updated.Version, "every mutation bumps conversation_version")
	assert.ElementsMatch(t, []uuid.UUID{caller, first, second}, updated.ActiveParticipantIDs())

	records := fixture.outbox(t)
	require.Len(t, records, 2)
	changed := conversationEvent(t, records, events.ChangeParticipantsChanged)
	payload := decodeConversationPayload(t, changed)
	assert.Equal(t, 2, payload.ConversationVersion)
	assert.ElementsMatch(t, []string{second.String()}, payload.AddedUserIDs, "existing participants are ignored")
	assert.Empty(t, payload.RemovedUserIDs)
	assert.ElementsMatch(t, []string{caller.String(), first.String(), second.String()}, payload.ParticipantUserIDs)
	assert.Equal(t, events.CorrelationFor(group.ID), changed.CorrelationID)

	// Adding only existing participants changes nothing.
	unchanged, err := fixture.service.AddParticipants(context.Background(), caller, group.ID, []uuid.UUID{first}, "")
	require.NoError(t, err)
	assert.Equal(t, 2, unchanged.Version)
	assert.Len(t, fixture.outbox(t), 2)
}

func TestAddParticipantsRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prepare    func(t *testing.T, fixture *conversationFixture, caller uuid.UUID) (uuid.UUID, uuid.UUID, []uuid.UUID)
		wantCode   models.ErrorCode
		wantStatus int
		actor      string
	}{
		{
			name: "caller is not a participant",
			prepare: func(t *testing.T, fixture *conversationFixture, caller uuid.UUID) (uuid.UUID, uuid.UUID, []uuid.UUID) {
				t.Helper()
				friend := uuid.New()
				fixture.makeFriends(t, caller, friend)
				group, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
					CallerID: caller, ParticipantIDs: []uuid.UUID{friend},
				})
				require.NoError(t, err)
				return group.ID, uuid.New(), []uuid.UUID{friend}
			},
			wantCode:   models.CodeNotAParticipant,
			wantStatus: 403,
			actor:      "outsider",
		},
		{
			name: "added user is not the caller's friend",
			prepare: func(t *testing.T, fixture *conversationFixture, caller uuid.UUID) (uuid.UUID, uuid.UUID, []uuid.UUID) {
				t.Helper()
				friend := uuid.New()
				stranger := uuid.New()
				fixture.makeFriends(t, caller, friend)
				fixture.makeFriends(t, stranger, friend)
				group, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
					CallerID: caller, ParticipantIDs: []uuid.UUID{friend},
				})
				require.NoError(t, err)
				return group.ID, uuid.New(), []uuid.UUID{stranger}
			},
			wantCode:   models.CodeNotFriends,
			wantStatus: 422,
			actor:      "caller",
		},
		{
			name: "group full",
			prepare: func(t *testing.T, fixture *conversationFixture, caller uuid.UUID) (uuid.UUID, uuid.UUID, []uuid.UUID) {
				t.Helper()
				initial := make([]uuid.UUID, 0, conversation.MaxGroupFriends)
				for i := 0; i < conversation.MaxGroupFriends; i++ {
					friend := uuid.New()
					fixture.becomeFriends(t, caller, friend)
					initial = append(initial, friend)
				}
				group, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
					CallerID: caller, ParticipantIDs: initial,
				})
				require.NoError(t, err)

				extra := uuid.New()
				fixture.becomeFriends(t, caller, extra)
				return group.ID, uuid.New(), []uuid.UUID{extra}
			},
			wantCode:   models.CodeGroupFull,
			wantStatus: 422,
			actor:      "caller",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := newConversationFixture(t)
			caller := uuid.New()
			conversationID, outsider, userIDs := test.prepare(t, fixture, caller)
			eventsBefore := len(fixture.outbox(t))

			actor := caller
			if test.actor == "outsider" {
				actor = outsider
			}
			_, err := fixture.service.AddParticipants(context.Background(), actor, conversationID, userIDs, "")
			require.Error(t, err)
			domainErr, ok := models.AsDomainError(err)
			require.True(t, ok)
			assert.Equal(t, test.wantCode, domainErr.Code)
			assert.Equal(t, test.wantStatus, domainErr.Status)
			assert.Len(t, fixture.outbox(t), eventsBefore, "an addition is all-or-nothing")
		})
	}
}

func TestAddParticipantsToDirectConversationIsRejected(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, friend, extra := uuid.New(), uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, friend, extra)

	direct, _, err := fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{CallerID: caller, FriendID: friend})
	require.NoError(t, err)

	_, err = fixture.service.AddParticipants(context.Background(), caller, direct.ID, []uuid.UUID{extra}, "")
	require.Error(t, err)
	domainErr, ok := models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, models.CodeGroupOnlyOperation, domainErr.Code)
	assert.Equal(t, 422, domainErr.Status)
}

func TestLeaveGroupConversation(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, first, second := uuid.New(), uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, first, second)

	group, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
		CallerID: caller, ParticipantIDs: []uuid.UUID{first, second},
	})
	require.NoError(t, err)

	require.NoError(t, fixture.service.Leave(context.Background(), first, group.ID, ""))
	records := fixture.outbox(t)
	require.Len(t, records, 2)
	payload := decodeConversationPayload(t, conversationEvent(t, records, events.ChangeParticipantsChanged))
	assert.Equal(t, 2, payload.ConversationVersion)
	assert.Equal(t, []string{first.String()}, payload.RemovedUserIDs)
	assert.NotContains(t, payload.ParticipantUserIDs, first.String())
	assert.Len(t, payload.ParticipantUserIDs, 2)

	// Left conversations no longer appear in the caller's list.
	left, _, err := fixture.service.List(context.Background(), first, nil, models.MaxPageLimit)
	require.NoError(t, err)
	assert.Empty(t, left)

	// They may not leave twice.
	err = fixture.service.Leave(context.Background(), first, group.ID, "")
	require.Error(t, err)
	domainErr, ok := models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, 404, domainErr.Status)
}

func TestLeaveDirectConversationIsRejected(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, friend := uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, friend)
	direct, _, err := fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{CallerID: caller, FriendID: friend})
	require.NoError(t, err)

	err = fixture.service.Leave(context.Background(), caller, direct.ID, "")
	require.Error(t, err)
	domainErr, ok := models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, models.CodeGroupOnlyOperation, domainErr.Code)
	assert.Equal(t, 422, domainErr.Status)

	// An unknown conversation is a 404.
	err = fixture.service.Leave(context.Background(), caller, uuid.New(), "")
	require.Error(t, err)
	domainErr, ok = models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, 404, domainErr.Status)
}

func TestLastParticipantLeavingKeepsTheConversation(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, friend := uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, friend)
	group, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
		CallerID: caller, ParticipantIDs: []uuid.UUID{friend},
	})
	require.NoError(t, err)

	require.NoError(t, fixture.service.Leave(context.Background(), caller, group.ID, ""))
	require.NoError(t, fixture.service.Leave(context.Background(), friend, group.ID, ""))

	records := fixture.outbox(t)
	require.Len(t, records, 3)
	var last events.ConversationPayload
	for _, record := range records {
		payload := decodeConversationPayload(t, record)
		if payload.ConversationVersion == 3 {
			last = payload
		}
	}
	assert.Empty(t, last.ParticipantUserIDs, "the empty conversation survives with no participants")
	assert.Equal(t, []string{friend.String()}, last.RemovedUserIDs)
}

func TestRenameGroupConversation(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, friend, outsider := uuid.New(), uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, friend)
	group, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
		CallerID: caller, ParticipantIDs: []uuid.UUID{friend},
	})
	require.NoError(t, err)

	renamed, err := fixture.service.Rename(context.Background(), friend, group.ID, "  Book Club ", "")
	require.NoError(t, err)
	require.NotNil(t, renamed.Name)
	assert.Equal(t, "Book Club", *renamed.Name)
	assert.Equal(t, 2, renamed.Version)

	records := fixture.outbox(t)
	require.Len(t, records, 2)
	payload := decodeConversationPayload(t, conversationEvent(t, records, events.ChangeRenamed))
	assert.Equal(t, 2, payload.ConversationVersion)
	require.NotNil(t, payload.Name)
	assert.Equal(t, "Book Club", *payload.Name)

	_, err = fixture.service.Rename(context.Background(), outsider, group.ID, "Nope", "")
	domainErr, ok := models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, models.CodeNotAParticipant, domainErr.Code)
	assert.Equal(t, 403, domainErr.Status)

	_, err = fixture.service.Rename(context.Background(), caller, group.ID, "   ", "")
	domainErr, ok = models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, models.CodeInvalidName, domainErr.Code)
	assert.Equal(t, 422, domainErr.Status)

	tooLong := ""
	for i := 0; i < conversation.MaxNameLength+1; i++ {
		tooLong += "x"
	}
	_, err = fixture.service.Rename(context.Background(), caller, group.ID, tooLong, "")
	domainErr, ok = models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, models.CodeInvalidName, domainErr.Code)
}

func TestRenameDirectConversationIsRejected(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, friend := uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, friend)
	direct, _, err := fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{CallerID: caller, FriendID: friend})
	require.NoError(t, err)

	_, err = fixture.service.Rename(context.Background(), caller, direct.ID, "Group-ish", "")
	require.Error(t, err)
	domainErr, ok := models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, models.CodeGroupOnlyOperation, domainErr.Code)

	_, err = fixture.service.Rename(context.Background(), caller, uuid.New(), "Missing", "")
	domainErr, ok = models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, 404, domainErr.Status)
}

func TestListConversationsExcludesLeftAndPaginates(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, friend, other := uuid.New(), uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, friend, other)

	firstGroup, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
		CallerID: caller, ParticipantIDs: []uuid.UUID{friend},
	})
	require.NoError(t, err)
	fixture.clock.Advance(time.Minute)
	direct, _, err := fixture.service.OpenDirect(context.Background(), conversation.OpenDirectInput{CallerID: caller, FriendID: other})
	require.NoError(t, err)

	conversations, _, err := fixture.service.List(context.Background(), caller, nil, models.MaxPageLimit)
	require.NoError(t, err)
	require.Len(t, conversations, 2)
	assert.Equal(t, direct.ID, conversations[0].ID, "newest conversation first")

	require.NoError(t, fixture.service.Leave(context.Background(), caller, firstGroup.ID, ""))
	conversations, _, err = fixture.service.List(context.Background(), caller, nil, models.MaxPageLimit)
	require.NoError(t, err)
	require.Len(t, conversations, 1)
	assert.Equal(t, direct.ID, conversations[0].ID)

	page, next, err := fixture.service.List(context.Background(), other, nil, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Nil(t, next)
}

func TestConversationRejoinAfterLeaving(t *testing.T) {
	t.Parallel()

	fixture := newConversationFixture(t)
	caller, friend := uuid.New(), uuid.New()
	fixture.makeFriends(t, caller, friend)
	group, err := fixture.service.CreateGroup(context.Background(), conversation.CreateGroupInput{
		CallerID: caller, ParticipantIDs: []uuid.UUID{friend},
	})
	require.NoError(t, err)

	require.NoError(t, fixture.service.Leave(context.Background(), friend, group.ID, ""))
	updated, err := fixture.service.AddParticipants(context.Background(), caller, group.ID, []uuid.UUID{friend}, "")
	require.NoError(t, err)
	assert.True(t, updated.IsActiveParticipant(friend))
	assert.ElementsMatch(t, []uuid.UUID{caller, friend}, updated.ActiveParticipantIDs())
}
