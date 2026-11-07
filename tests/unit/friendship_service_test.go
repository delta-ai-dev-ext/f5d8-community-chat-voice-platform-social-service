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

	"social_service/internal/events"
	"social_service/internal/friendship"
	"social_service/internal/models"
	"social_service/internal/repository"
	"social_service/tests/mocks"
)

// baseTime is the injected clock instant for every unit test.
var baseTime = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

type friendshipFixture struct {
	store   *repository.MemoryStore
	clock   *mocks.FixedClock
	service *friendship.Service
	metrics *friendship.Metrics
}

func newFriendshipFixture(t *testing.T) *friendshipFixture {
	t.Helper()
	store := repository.NewMemoryStore()
	clock := mocks.NewFixedClock(baseTime)
	metrics := friendship.NewMetrics(prometheus.NewRegistry())
	service := friendship.NewService(store, clock, zap.NewNop(), metrics)
	t.Cleanup(store.Close)
	return &friendshipFixture{store: store, clock: clock, service: service, metrics: metrics}
}

func (f *friendshipFixture) outbox(t *testing.T) []models.OutboxRecord {
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

func (f *friendshipFixture) becomeFriends(t *testing.T, first, second uuid.UUID) {
	t.Helper()
	lesser, greater := models.CanonicalPair(first, second)
	require.NoError(t, f.store.InTx(context.Background(), func(ctx context.Context, tx repository.Tx) error {
		return tx.InsertFriendship(ctx, models.Friendship{UserIDA: lesser, UserIDB: greater, CreatedAt: f.clock.Now()})
	}))
}

func (f *friendshipFixture) listRequests(t *testing.T, userID uuid.UUID, direction models.RequestDirection) []models.FriendRequest {
	t.Helper()
	requests, _, err := f.service.ListRequests(context.Background(), userID, direction, nil, models.MaxPageLimit)
	require.NoError(t, err)
	return requests
}

func decodeFriendRequestPayload(t *testing.T, record models.OutboxRecord) events.FriendRequestPayload {
	t.Helper()
	var payload events.FriendRequestPayload
	require.NoError(t, json.Unmarshal(record.Payload, &payload))
	return payload
}

// friendRequestEvent returns the single outbox event carrying the given status.
// Outbox rows with an identical created_at are ordered by id, so tests must
// select by content rather than by position.
func friendRequestEvent(t *testing.T, records []models.OutboxRecord, status models.FriendRequestStatus) models.OutboxRecord {
	t.Helper()
	var matches []models.OutboxRecord
	for _, record := range records {
		if decodeFriendRequestPayload(t, record).Status == string(status) {
			matches = append(matches, record)
		}
	}
	require.Len(t, matches, 1, "expected exactly one %s event", status)
	return matches[0]
}

func TestSendFriendRequestCreatesPendingRequest(t *testing.T) {
	t.Parallel()

	fixture := newFriendshipFixture(t)
	sender, recipient := uuid.New(), uuid.New()

	request, err := fixture.service.Send(context.Background(), friendship.SendInput{
		CallerID:    sender,
		RecipientID: recipient,
		TraceID:     "0123456789abcdef0123456789abcdef",
	})
	require.NoError(t, err)

	assert.Equal(t, sender, request.SenderUserID)
	assert.Equal(t, recipient, request.RecipientUserID)
	assert.Equal(t, models.FriendRequestPending, request.Status)
	assert.Equal(t, baseTime, request.CreatedAt)

	records := fixture.outbox(t)
	require.Len(t, records, 1)
	record := records[0]
	assert.Equal(t, events.EventTypeFriendRequestChanged, record.EventType)
	assert.Equal(t, events.Topic, record.Topic)
	assert.Equal(t, recipient.String(), record.PartitionKey, "EVT-007 is keyed by recipient_user_id")
	assert.Equal(t, models.AggregateTypeFriendRequest, record.AggregateType)
	assert.Equal(t, request.ID, record.AggregateID)
	assert.Equal(t, events.CorrelationFor(request.ID), record.CorrelationID)
	assert.Nil(t, record.CausationID, "the first event of a flow has causation_id = null")

	payload := decodeFriendRequestPayload(t, record)
	assert.Equal(t, request.ID.String(), payload.RequestID)
	assert.Equal(t, sender.String(), payload.SenderUserID)
	assert.Equal(t, recipient.String(), payload.RecipientUserID)
	assert.Equal(t, string(models.FriendRequestPending), payload.Status)

	message, err := events.BuildMessage(record, baseTime)
	require.NoError(t, err)
	assert.Equal(t, events.Producer, message.Envelope.Producer)
	assert.Equal(t, "0123456789abcdef0123456789abcdef", message.Envelope.TraceID)
	assert.Equal(t, events.SchemaRef(events.EventTypeFriendRequestChanged, events.EventVersion), message.Envelope.SchemaRef)
}

func TestSendFriendRequestRejections(t *testing.T) {
	t.Parallel()

	incomingPending := func(t *testing.T, fixture *friendshipFixture, caller, other uuid.UUID) {
		t.Helper()
		_, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: other, RecipientID: caller})
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		setup       func(t *testing.T, fixture *friendshipFixture, caller, other uuid.UUID)
		recipient   func(caller, other uuid.UUID) uuid.UUID
		wantCode    models.ErrorCode
		wantStatus  int
		wantRecords int
	}{
		{
			name:       "self request",
			recipient:  func(caller, _ uuid.UUID) uuid.UUID { return caller },
			wantCode:   models.CodeSelfRequest,
			wantStatus: 422,
		},
		{
			name: "already friends",
			setup: func(t *testing.T, fixture *friendshipFixture, caller, other uuid.UUID) {
				fixture.becomeFriends(t, caller, other)
			},
			wantCode:   models.CodeAlreadyFriends,
			wantStatus: 409,
		},
		{
			name: "pending request from me",
			setup: func(t *testing.T, fixture *friendshipFixture, caller, other uuid.UUID) {
				_, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: caller, RecipientID: other})
				require.NoError(t, err)
			},
			wantCode:    models.CodeRequestPending,
			wantStatus:  409,
			wantRecords: 1,
		},
		{
			name: "pending request from them",
			setup: func(t *testing.T, fixture *friendshipFixture, caller, other uuid.UUID) {
				incomingPending(t, fixture, caller, other)
			},
			wantCode:    models.CodeRequestPending,
			wantStatus:  409,
			wantRecords: 1,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := newFriendshipFixture(t)
			caller, other := uuid.New(), uuid.New()
			if test.setup != nil {
				test.setup(t, fixture, caller, other)
			}
			recipient := other
			if test.recipient != nil {
				recipient = test.recipient(caller, other)
			}

			_, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: caller, RecipientID: recipient})
			require.Error(t, err)

			domainErr, ok := models.AsDomainError(err)
			require.True(t, ok, "expected a domain error, got %v", err)
			assert.Equal(t, test.wantCode, domainErr.Code)
			assert.Equal(t, test.wantStatus, domainErr.Status)
			assert.Len(t, fixture.outbox(t), test.wantRecords, "no event may be emitted for a rejected request")
		})
	}
}

func TestSendFriendRequestIdempotencyKey(t *testing.T) {
	t.Parallel()

	t.Run("replay returns the original request", func(t *testing.T) {
		t.Parallel()

		fixture := newFriendshipFixture(t)
		sender, recipient := uuid.New(), uuid.New()
		body := []byte(`{"recipient_user_id":"` + recipient.String() + `"}`)
		key := models.NewIdempotencyKey("key-1", body)

		first, err := fixture.service.Send(context.Background(), friendship.SendInput{
			CallerID: sender, RecipientID: recipient, IdempotencyKey: key,
		})
		require.NoError(t, err)

		second, err := fixture.service.Send(context.Background(), friendship.SendInput{
			CallerID: sender, RecipientID: recipient, IdempotencyKey: key,
		})
		require.NoError(t, err)

		assert.Equal(t, first.ID, second.ID)
		assert.Len(t, fixture.outbox(t), 1, "a replay must not publish a second event")
	})

	t.Run("same key with a different body is rejected", func(t *testing.T) {
		t.Parallel()

		fixture := newFriendshipFixture(t)
		sender := uuid.New()
		first := uuid.New()
		second := uuid.New()

		_, err := fixture.service.Send(context.Background(), friendship.SendInput{
			CallerID: sender, RecipientID: first,
			IdempotencyKey: models.NewIdempotencyKey("key-2", []byte(`{"recipient_user_id":"`+first.String()+`"}`)),
		})
		require.NoError(t, err)

		_, err = fixture.service.Send(context.Background(), friendship.SendInput{
			CallerID: sender, RecipientID: second,
			IdempotencyKey: models.NewIdempotencyKey("key-2", []byte(`{"recipient_user_id":"`+second.String()+`"}`)),
		})
		require.Error(t, err)
		domainErr, ok := models.AsDomainError(err)
		require.True(t, ok)
		assert.Equal(t, models.CodeIdempotencyKeyReuse, domainErr.Code)
		assert.Equal(t, 409, domainErr.Status)
		assert.Len(t, fixture.outbox(t), 1)
	})
}

func TestAcceptFriendRequestLifecycle(t *testing.T) {
	t.Parallel()

	fixture := newFriendshipFixture(t)
	sender, recipient := uuid.New(), uuid.New()

	request, err := fixture.service.Send(context.Background(), friendship.SendInput{
		CallerID: sender, RecipientID: recipient, TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	require.NoError(t, err)
	sentEvent := fixture.outbox(t)[0]

	friendshipRow, err := fixture.service.Accept(context.Background(), recipient, request.ID, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, err)

	lesser, greater := models.CanonicalPair(sender, recipient)
	assert.Equal(t, lesser, friendshipRow.UserIDA)
	assert.Equal(t, greater, friendshipRow.UserIDB)
	assert.Equal(t, baseTime, friendshipRow.CreatedAt)

	records := fixture.outbox(t)
	require.Len(t, records, 2)
	acceptedEvent := friendRequestEvent(t, records, models.FriendRequestAccepted)
	payload := decodeFriendRequestPayload(t, acceptedEvent)
	assert.Equal(t, string(models.FriendRequestAccepted), payload.Status)
	assert.Equal(t, events.CorrelationFor(request.ID), acceptedEvent.CorrelationID, "one correlation id per lifecycle")
	require.NotNil(t, acceptedEvent.CausationID)
	assert.Equal(t, sentEvent.ID, *acceptedEvent.CausationID)

	// Repeat accept is idempotent: same friendship, no second event.
	again, err := fixture.service.Accept(context.Background(), recipient, request.ID, "")
	require.NoError(t, err)
	assert.Equal(t, friendshipRow, again)
	assert.Len(t, fixture.outbox(t), 2)

	// The sender may not accept their own outbound request.
	otherRequest, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: sender, RecipientID: uuid.New()})
	require.NoError(t, err)
	_, err = fixture.service.Accept(context.Background(), sender, otherRequest.ID, "")
	require.Error(t, err)
	domainErr, ok := models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, 404, domainErr.Status)

	// Unknown request id.
	_, err = fixture.service.Accept(context.Background(), recipient, uuid.New(), "")
	require.Error(t, err)
	domainErr, ok = models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, 404, domainErr.Status)
}

func TestAcceptRejectsNonPendingRequest(t *testing.T) {
	t.Parallel()

	fixture := newFriendshipFixture(t)
	sender, recipient := uuid.New(), uuid.New()
	request, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: sender, RecipientID: recipient})
	require.NoError(t, err)
	require.NoError(t, fixture.service.Decline(context.Background(), recipient, request.ID, ""))

	_, err = fixture.service.Accept(context.Background(), recipient, request.ID, "")
	require.Error(t, err)
	domainErr, ok := models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, models.CodeRequestNotPending, domainErr.Code)
	assert.Equal(t, 409, domainErr.Status)
}

func TestDeclineAndCancelFriendRequest(t *testing.T) {
	t.Parallel()

	t.Run("decline by the recipient", func(t *testing.T) {
		t.Parallel()

		fixture := newFriendshipFixture(t)
		sender, recipient := uuid.New(), uuid.New()
		request, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: sender, RecipientID: recipient})
		require.NoError(t, err)

		require.NoError(t, fixture.service.Decline(context.Background(), recipient, request.ID, ""))
		records := fixture.outbox(t)
		require.Len(t, records, 2)
		assert.Equal(t, models.FriendRequestDeclined, models.FriendRequestStatus(decodeFriendRequestPayload(t, friendRequestEvent(t, records, models.FriendRequestDeclined)).Status))

		// Declining again is a conflict: the state machine is forward-only.
		err = fixture.service.Decline(context.Background(), recipient, request.ID, "")
		domainErr, ok := models.AsDomainError(err)
		require.True(t, ok)
		assert.Equal(t, models.CodeRequestNotPending, domainErr.Code)
		assert.Equal(t, 409, domainErr.Status)
	})

	t.Run("decline by the sender is not visible", func(t *testing.T) {
		t.Parallel()

		fixture := newFriendshipFixture(t)
		sender, recipient := uuid.New(), uuid.New()
		request, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: sender, RecipientID: recipient})
		require.NoError(t, err)

		err = fixture.service.Decline(context.Background(), sender, request.ID, "")
		domainErr, ok := models.AsDomainError(err)
		require.True(t, ok)
		assert.Equal(t, 404, domainErr.Status)
	})

	t.Run("cancel by the sender", func(t *testing.T) {
		t.Parallel()

		fixture := newFriendshipFixture(t)
		sender, recipient := uuid.New(), uuid.New()
		request, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: sender, RecipientID: recipient})
		require.NoError(t, err)

		require.NoError(t, fixture.service.Cancel(context.Background(), sender, request.ID, ""))
		records := fixture.outbox(t)
		require.Len(t, records, 2)
		assert.Equal(t, models.FriendRequestCancelled, models.FriendRequestStatus(decodeFriendRequestPayload(t, friendRequestEvent(t, records, models.FriendRequestCancelled)).Status))
	})

	t.Run("cancel by the recipient is not visible", func(t *testing.T) {
		t.Parallel()

		fixture := newFriendshipFixture(t)
		sender, recipient := uuid.New(), uuid.New()
		request, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: sender, RecipientID: recipient})
		require.NoError(t, err)

		err = fixture.service.Cancel(context.Background(), recipient, request.ID, "")
		domainErr, ok := models.AsDomainError(err)
		require.True(t, ok)
		assert.Equal(t, 404, domainErr.Status)
	})

	t.Run("cancel an accepted request is a conflict", func(t *testing.T) {
		t.Parallel()

		fixture := newFriendshipFixture(t)
		sender, recipient := uuid.New(), uuid.New()
		request, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: sender, RecipientID: recipient})
		require.NoError(t, err)
		_, err = fixture.service.Accept(context.Background(), recipient, request.ID, "")
		require.NoError(t, err)

		err = fixture.service.Cancel(context.Background(), sender, request.ID, "")
		domainErr, ok := models.AsDomainError(err)
		require.True(t, ok)
		assert.Equal(t, models.CodeRequestNotPending, domainErr.Code)
	})
}

func TestRemoveFriend(t *testing.T) {
	t.Parallel()

	fixture := newFriendshipFixture(t)
	caller, friend := uuid.New(), uuid.New()
	fixture.becomeFriends(t, caller, friend)

	require.NoError(t, fixture.service.RemoveFriend(context.Background(), caller, friend))
	assert.Empty(t, fixture.outbox(t), "unfriending emits no event")

	friends, _, err := fixture.service.ListFriends(context.Background(), friend, nil, models.MaxPageLimit)
	require.NoError(t, err)
	assert.Empty(t, friends, "removal is mutual and immediate")

	err = fixture.service.RemoveFriend(context.Background(), caller, friend)
	require.Error(t, err)
	domainErr, ok := models.AsDomainError(err)
	require.True(t, ok)
	assert.Equal(t, models.CodeNotFriends, domainErr.Code)
	assert.Equal(t, 404, domainErr.Status)
}

func TestListFriendRequests(t *testing.T) {
	t.Parallel()

	fixture := newFriendshipFixture(t)
	caller := uuid.New()
	incomingOne, incomingTwo, outgoing := uuid.New(), uuid.New(), uuid.New()

	_, err := fixture.service.Send(context.Background(), friendship.SendInput{CallerID: incomingOne, RecipientID: caller})
	require.NoError(t, err)
	_, err = fixture.service.Send(context.Background(), friendship.SendInput{CallerID: incomingTwo, RecipientID: caller})
	require.NoError(t, err)
	_, err = fixture.service.Send(context.Background(), friendship.SendInput{CallerID: caller, RecipientID: outgoing})
	require.NoError(t, err)

	incoming := fixture.listRequests(t, caller, models.DirectionIncoming)
	require.Len(t, incoming, 2)
	for _, request := range incoming {
		assert.Equal(t, caller, request.RecipientUserID)
		assert.True(t, request.CreatedAt.Equal(baseTime))
	}

	sent := fixture.listRequests(t, caller, models.DirectionOutgoing)
	require.Len(t, sent, 1)
	assert.Equal(t, outgoing, sent[0].RecipientUserID)

	// Pagination: limit 1 returns one row plus a cursor that yields the second.
	firstPage, next, err := fixture.service.ListRequests(context.Background(), caller, models.DirectionIncoming, nil, 1)
	require.NoError(t, err)
	require.Len(t, firstPage, 1)
	require.NotNil(t, next)

	cursor, ok, err := models.ParseCursor(*next)
	require.NoError(t, err)
	require.True(t, ok)
	secondPage, last, err := fixture.service.ListRequests(context.Background(), caller, models.DirectionIncoming, &cursor, 1)
	require.NoError(t, err)
	require.Len(t, secondPage, 1)
	assert.Nil(t, last, "the final page has no next_cursor")
	assert.NotEqual(t, firstPage[0].ID, secondPage[0].ID)
}

func TestListFriendsIsOrderedAndPaginated(t *testing.T) {
	t.Parallel()

	fixture := newFriendshipFixture(t)
	caller := uuid.New()
	first, second := uuid.New(), uuid.New()

	fixture.becomeFriends(t, caller, first)
	fixture.clock.Advance(time.Minute)
	fixture.becomeFriends(t, caller, second)

	friends, next, err := fixture.service.ListFriends(context.Background(), caller, nil, models.MaxPageLimit)
	require.NoError(t, err)
	require.Len(t, friends, 2)
	assert.Nil(t, next)
	assert.Equal(t, second, mustOther(t, friends[0], caller), "newest friendship first")

	page, next, err := fixture.service.ListFriends(context.Background(), caller, nil, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.NotNil(t, next)

	cursor, ok, err := models.ParseCursor(*next)
	require.NoError(t, err)
	require.True(t, ok)
	rest, last, err := fixture.service.ListFriends(context.Background(), caller, &cursor, 1)
	require.NoError(t, err)
	require.Len(t, rest, 1)
	assert.Nil(t, last)
	assert.Equal(t, first, mustOther(t, rest[0], caller))
}

func mustOther(t *testing.T, friendship models.Friendship, viewer uuid.UUID) uuid.UUID {
	t.Helper()
	other, ok := friendship.Other(viewer)
	require.True(t, ok)
	return other
}
