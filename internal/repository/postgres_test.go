package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"social_service/internal/models"
)

// fakeTx is a scripted pgx.Tx. It records every statement so the tests can
// assert the SQL shape (parameterized, canonical columns) and the argument
// order without a PostgreSQL server.
type fakeTx struct {
	execSQL  []string
	execArgs [][]any
	execTag  pgconn.CommandTag
	execErr  error

	querySQL   string
	queryArgs  []any
	queryRows  *fakeRows
	queryErr   error
	queryCalls int

	rowValues []any
	rowErr    error
}

func (f *fakeTx) Begin(context.Context) (pgx.Tx, error) { return f, nil }
func (f *fakeTx) Commit(context.Context) error          { return nil }
func (f *fakeTx) Rollback(context.Context) error        { return nil }
func (f *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (f *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (f *fakeTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (f *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (f *fakeTx) Conn() *pgx.Conn { return nil }

func (f *fakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.execSQL = append(f.execSQL, sql)
	f.execArgs = append(f.execArgs, args)
	if f.execErr != nil {
		return pgconn.CommandTag{}, f.execErr
	}
	return f.execTag, nil
}

func (f *fakeTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.querySQL = sql
	f.queryArgs = args
	f.queryCalls++
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	if f.queryRows == nil {
		return newFakeRows(), nil
	}
	return f.queryRows, nil
}

func (f *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row {
	return &fakeRow{values: f.rowValues, err: f.rowErr}
}

type fakeRow struct {
	values []any
	err    error
}

func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return assignValues(dest, r.values)
}

type fakeRows struct {
	rows   [][]any
	index  int
	err    error
	closed bool
}

func newFakeRows(rows ...[]any) *fakeRows {
	return &fakeRows{rows: rows, index: -1}
}

func (r *fakeRows) Next() bool {
	if r.index+1 >= len(r.rows) {
		r.closed = true
		return false
	}
	r.index++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.index < 0 || r.index >= len(r.rows) {
		return errors.New("fake rows: Scan called before Next")
	}
	return assignValues(dest, r.rows[r.index])
}

func (r *fakeRows) Close()                                       { r.closed = true }
func (r *fakeRows) Err() error                                   { return r.err }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return r.rows[r.index], nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) TypeMap() *pgtype.Map                         { return nil }

// assignValues emulates pgx scanning: assignable values are set directly and
// values assigned into pointer destinations are boxed.
func assignValues(dest []any, values []any) error {
	if len(dest) != len(values) {
		return fmt.Errorf("fake scan: %d destinations for %d values", len(dest), len(values))
	}
	for i, target := range dest {
		if err := assignValue(target, values[i]); err != nil {
			return err
		}
	}
	return nil
}

func assignValue(target, value any) error {
	pointer := reflect.ValueOf(target)
	if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
		return fmt.Errorf("fake scan: destination %T is not a pointer", target)
	}
	elem := pointer.Elem()
	if value == nil {
		elem.SetZero()
		return nil
	}
	source := reflect.ValueOf(value)
	if source.Type().AssignableTo(elem.Type()) {
		elem.Set(source)
		return nil
	}
	if elem.Kind() == reflect.Pointer && source.Type().AssignableTo(elem.Type().Elem()) {
		boxed := reflect.New(elem.Type().Elem())
		boxed.Elem().Set(source)
		elem.Set(boxed)
		return nil
	}
	return fmt.Errorf("fake scan: cannot assign %T to %s", value, elem.Type())
}

var (
	testTime         = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	testFirstUser    = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	testSecondUser   = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	testRequestID    = uuid.MustParse("33333333-3333-4333-8333-333333333333")
	testConversation = uuid.MustParse("44444444-4444-4444-8444-444444444444")
)

func friendRequestRow() []any {
	return []any{testRequestID, testFirstUser, testSecondUser, models.FriendRequestPending, testTime, testTime}
}

func TestPgTxFriendRequests(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := &fakeTx{execTag: pgconn.NewCommandTag("UPDATE 1")}

	require.NoError(t, (&pgTx{tx: tx}).InsertFriendRequest(ctx, models.FriendRequest{
		ID:              testRequestID,
		SenderUserID:    testFirstUser,
		RecipientUserID: testSecondUser,
		Status:          models.FriendRequestPending,
		CreatedAt:       testTime,
		UpdatedAt:       testTime,
	}))
	require.Len(t, tx.execSQL, 1)
	assert.Contains(t, tx.execSQL[0], "INSERT INTO friend_request")
	assert.NotContains(t, tx.execSQL[0], testFirstUser.String(), "values are bound, never interpolated")
	assert.Len(t, tx.execArgs[0], 6)

	tx.rowValues = friendRequestRow()
	request, err := (&pgTx{tx: tx}).GetFriendRequest(ctx, testRequestID)
	require.NoError(t, err)
	assert.Equal(t, testRequestID, request.ID)
	assert.Equal(t, models.FriendRequestPending, request.Status)
	assert.True(t, request.CreatedAt.Equal(testTime))

	lockingTx := &fakeTx{rowValues: friendRequestRow()}
	_, err = (&pgTx{tx: lockingTx}).LockFriendRequest(ctx, testRequestID)
	require.NoError(t, err)

	require.NoError(t, (&pgTx{tx: tx}).UpdateFriendRequestStatus(ctx, testRequestID, models.FriendRequestAccepted, testTime))
	assert.Contains(t, tx.execSQL[len(tx.execSQL)-1], "UPDATE friend_request SET status")

	noRows := &fakeTx{execTag: pgconn.NewCommandTag("UPDATE 0")}
	err = (&pgTx{tx: noRows}).UpdateFriendRequestStatus(ctx, testRequestID, models.FriendRequestAccepted, testTime)
	assert.ErrorIs(t, err, ErrNoRows)

	tx.rowValues = friendRequestRow()
	between, err := (&pgTx{tx: tx}).PendingFriendRequestBetween(ctx, testFirstUser, testSecondUser)
	require.NoError(t, err)
	assert.Equal(t, testRequestID, between.ID)

	tx.rowErr = pgx.ErrNoRows
	_, err = (&pgTx{tx: tx}).PendingFriendRequestBetween(ctx, testFirstUser, testSecondUser)
	assert.ErrorIs(t, err, ErrNoRows)
	tx.rowErr = nil

	t.Run("list incoming", func(t *testing.T) {
		t.Parallel()

		listTx := &fakeTx{queryRows: newFakeRows(friendRequestRow())}
		requests, err := (&pgTx{tx: listTx}).ListFriendRequests(ctx, testSecondUser, models.DirectionIncoming, nil, PaginationFetchSize(50))
		require.NoError(t, err)
		require.Len(t, requests, 1)
		assert.Contains(t, listTx.querySQL, "recipient_user_id = $1")
		assert.Contains(t, listTx.querySQL, "ORDER BY created_at DESC, id DESC")
		assert.NotContains(t, listTx.querySQL, "created_at, id) <", "the first page carries no cursor predicate")
	})

	t.Run("list outgoing with cursor", func(t *testing.T) {
		t.Parallel()

		cursor := models.Cursor{CreatedAt: testTime, ID: testRequestID}
		listTx := &fakeTx{queryRows: newFakeRows()}
		requests, err := (&pgTx{tx: listTx}).ListFriendRequests(ctx, testFirstUser, models.DirectionOutgoing, &cursor, 10)
		require.NoError(t, err)
		assert.Empty(t, requests)
		assert.Contains(t, listTx.querySQL, "sender_user_id = $1")
		assert.Contains(t, listTx.querySQL, "(created_at, id) < ($2, $3)")
		assert.Len(t, listTx.queryArgs, 4)
	})

	t.Run("list propagates row errors", func(t *testing.T) {
		t.Parallel()

		listTx := &fakeTx{queryRows: &fakeRows{err: errors.New("boom")}}
		_, err := (&pgTx{tx: listTx}).ListFriendRequests(ctx, testFirstUser, models.DirectionIncoming, nil, 10)
		require.Error(t, err)

		failingQuery := &fakeTx{queryErr: errors.New("boom")}
		_, err = (&pgTx{tx: failingQuery}).ListFriendRequests(ctx, testFirstUser, models.DirectionIncoming, nil, 10)
		require.Error(t, err)
	})

	t.Run("unique violation is translated", func(t *testing.T) {
		t.Parallel()

		uniqueTx := &fakeTx{execErr: &pgconn.PgError{Code: "23505", ConstraintName: "uq_friend_request_pending_pair"}}
		err := (&pgTx{tx: uniqueTx}).InsertFriendRequest(ctx, models.FriendRequest{
			ID: testRequestID, SenderUserID: testFirstUser, RecipientUserID: testSecondUser,
			Status: models.FriendRequestPending, CreatedAt: testTime, UpdatedAt: testTime,
		})
		assert.ErrorIs(t, err, ErrUniqueViolation)

		checkTx := &fakeTx{execErr: &pgconn.PgError{Code: "23514", ConstraintName: "friend_request_no_self"}}
		err = (&pgTx{tx: checkTx}).InsertFriendRequest(ctx, models.FriendRequest{ID: testRequestID})
		assert.ErrorIs(t, err, ErrCheckViolation)
	})
}

func TestPgTxFriendships(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := &fakeTx{execTag: pgconn.NewCommandTag("UPDATE 1")}

	lesser, greater := models.CanonicalPair(testSecondUser, testFirstUser)
	require.NoError(t, (&pgTx{tx: tx}).InsertFriendship(ctx, models.Friendship{UserIDA: lesser, UserIDB: greater, CreatedAt: testTime}))
	assert.Contains(t, tx.execSQL[0], "INSERT INTO friendship")
	assert.Equal(t, lesser, tx.execArgs[0][0])
	assert.Equal(t, greater, tx.execArgs[0][1])

	tx.rowValues = []any{lesser, greater, testTime}
	friendshipRow, err := (&pgTx{tx: tx}).GetFriendship(ctx, testFirstUser, testSecondUser)
	require.NoError(t, err)
	assert.Equal(t, lesser, friendshipRow.UserIDA)

	deleted, err := (&pgTx{tx: tx}).DeleteFriendship(ctx, testFirstUser, testSecondUser)
	require.NoError(t, err)
	assert.True(t, deleted)

	noneDeleted := &fakeTx{execTag: pgconn.NewCommandTag("DELETE 0")}
	deleted, err = (&pgTx{tx: noneDeleted}).DeleteFriendship(ctx, testFirstUser, testSecondUser)
	require.NoError(t, err)
	assert.False(t, deleted)

	t.Run("list with cursor", func(t *testing.T) {
		t.Parallel()

		cursor := models.Cursor{CreatedAt: testTime, ID: testSecondUser}
		listTx := &fakeTx{queryRows: newFakeRows([]any{lesser, greater, testTime})}
		rows, err := (&pgTx{tx: listTx}).ListFriendships(ctx, testFirstUser, &cursor, 5)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Contains(t, listTx.querySQL, "CASE WHEN user_id_a = $1 THEN user_id_b ELSE user_id_a END")
		assert.Contains(t, listTx.querySQL, "ORDER BY created_at DESC")
	})

	countTx := &fakeTx{rowValues: []any{int64(3)}}
	count, err := (&pgTx{tx: countTx}).CountFriendships(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	countErrTx := &fakeTx{rowErr: errors.New("boom")}
	_, err = (&pgTx{tx: countErrTx}).CountFriendships(ctx)
	require.Error(t, err)
}

func conversationRow(name *string, pairKey *string, version int) []any {
	return []any{testConversation, models.ConversationGroup, name, testFirstUser, pairKey, version, testTime, testTime}
}

func TestPgTxConversations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	name := "Book Club"

	t.Run("insert conversation and participants", func(t *testing.T) {
		t.Parallel()

		tx := &fakeTx{execTag: pgconn.NewCommandTag("INSERT 0 1")}
		err := (&pgTx{tx: tx}).InsertConversation(ctx, models.Conversation{
			ID:        testConversation,
			Kind:      models.ConversationGroup,
			Name:      &name,
			CreatedBy: testFirstUser,
			Version:   1,
			CreatedAt: testTime,
			UpdatedAt: testTime,
			Participants: []models.Participant{
				{ConversationID: testConversation, UserID: testFirstUser, JoinedAt: testTime},
				{ConversationID: testConversation, UserID: testSecondUser, JoinedAt: testTime},
			},
		})
		require.NoError(t, err)
		require.Len(t, tx.execSQL, 3, "one conversation row plus one row per participant")
		assert.Contains(t, tx.execSQL[0], "INSERT INTO conversation")
		assert.Contains(t, tx.execSQL[1], "INSERT INTO conversation_participant")

		uniqueTx := &fakeTx{execErr: &pgconn.PgError{Code: "23505", ConstraintName: "uq_direct_pair"}}
		err = (&pgTx{tx: uniqueTx}).InsertConversation(ctx, models.Conversation{ID: testConversation, Kind: models.ConversationDirect})
		assert.ErrorIs(t, err, ErrUniqueViolation)
	})

	t.Run("get conversation loads participants", func(t *testing.T) {
		t.Parallel()

		tx := &fakeTx{
			rowValues: conversationRow(&name, nil, 3),
			queryRows: newFakeRows(
				[]any{testConversation, testFirstUser, testTime, nil},
				[]any{testConversation, testSecondUser, testTime, testTime},
			),
		}
		conversation, err := (&pgTx{tx: tx}).GetConversation(ctx, testConversation)
		require.NoError(t, err)
		assert.Equal(t, testConversation, conversation.ID)
		require.Len(t, conversation.Participants, 2)
		assert.True(t, conversation.Participants[0].Active())
		assert.False(t, conversation.Participants[1].Active())
		assert.Equal(t, 3, conversation.Version)

		missing := &fakeTx{rowErr: pgx.ErrNoRows}
		_, err = (&pgTx{tx: missing}).GetConversation(ctx, testConversation)
		assert.ErrorIs(t, err, ErrNoRows)

		brokenParticipants := &fakeTx{rowValues: conversationRow(nil, nil, 1), queryErr: errors.New("boom")}
		_, err = (&pgTx{tx: brokenParticipants}).GetConversation(ctx, testConversation)
		require.Error(t, err)
	})

	t.Run("direct conversation by pair key", func(t *testing.T) {
		t.Parallel()

		pairKey := testFirstUser.String() + "|" + testSecondUser.String()
		tx := &fakeTx{
			rowValues: conversationRow(nil, &pairKey, 1),
			queryRows: newFakeRows([]any{testConversation, testFirstUser, testTime, nil}),
		}
		conversation, err := (&pgTx{tx: tx}).DirectConversationByPairKey(ctx, pairKey)
		require.NoError(t, err)
		require.NotNil(t, conversation.DirectPairKey)
		assert.Equal(t, pairKey, *conversation.DirectPairKey)

		noRows := &fakeTx{rowErr: pgx.ErrNoRows}
		_, err = (&pgTx{tx: noRows}).DirectConversationByPairKey(ctx, pairKey)
		assert.ErrorIs(t, err, ErrNoRows)

		broken := &fakeTx{rowValues: conversationRow(nil, &pairKey, 1), queryErr: errors.New("boom")}
		_, err = (&pgTx{tx: broken}).DirectConversationByPairKey(ctx, pairKey)
		require.Error(t, err)
	})

	t.Run("list conversations only returns active participations", func(t *testing.T) {
		t.Parallel()

		tx := &fakeTx{queryRows: newFakeRows(conversationRow(&name, nil, 2))}
		// The participants query reuses the scripted rows; disable it by
		// answering with an empty set on the second call.
		conversations, err := (&pgTx{tx: &participantsAwareTx{first: tx}}).ListConversations(ctx, testFirstUser, nil, 10)
		require.NoError(t, err)
		require.Len(t, conversations, 1)
		assert.Contains(t, tx.querySQL, "p.left_at IS NULL")
		assert.Contains(t, tx.querySQL, "ORDER BY c.created_at DESC, c.id DESC")

		cursor := models.Cursor{CreatedAt: testTime, ID: testConversation}
		cursorTx := &fakeTx{queryRows: newFakeRows()}
		empty, err := (&pgTx{tx: &participantsAwareTx{first: cursorTx}}).ListConversations(ctx, testFirstUser, &cursor, 10)
		require.NoError(t, err)
		assert.Empty(t, empty)
		assert.Contains(t, cursorTx.querySQL, "(c.created_at, c.id) < ($2, $3)")

		failing := &fakeTx{queryErr: errors.New("boom")}
		_, err = (&pgTx{tx: failing}).ListConversations(ctx, testFirstUser, nil, 10)
		require.Error(t, err)
	})

	t.Run("participant mutations", func(t *testing.T) {
		t.Parallel()

		activated := &fakeTx{execTag: pgconn.NewCommandTag("INSERT 0 1")}
		changed, err := (&pgTx{tx: activated}).UpsertParticipant(ctx, models.Participant{
			ConversationID: testConversation, UserID: testFirstUser, JoinedAt: testTime,
		})
		require.NoError(t, err)
		assert.True(t, changed)
		assert.Contains(t, activated.execSQL[0], "ON CONFLICT (conversation_id, user_id) DO UPDATE")

		ignored := &fakeTx{execTag: pgconn.NewCommandTag("INSERT 0 0")}
		changed, err = (&pgTx{tx: ignored}).UpsertParticipant(ctx, models.Participant{ConversationID: testConversation, UserID: testFirstUser, JoinedAt: testTime})
		require.NoError(t, err)
		assert.False(t, changed)

		failing := &fakeTx{execErr: errors.New("boom")}
		_, err = (&pgTx{tx: failing}).UpsertParticipant(ctx, models.Participant{ConversationID: testConversation, UserID: testFirstUser})
		require.Error(t, err)

		left, err := (&pgTx{tx: &fakeTx{execTag: pgconn.NewCommandTag("UPDATE 1")}}).MarkParticipantLeft(ctx, testConversation, testFirstUser, testTime)
		require.NoError(t, err)
		assert.True(t, left)

		notLeft, err := (&pgTx{tx: &fakeTx{execTag: pgconn.NewCommandTag("UPDATE 0")}}).MarkParticipantLeft(ctx, testConversation, testFirstUser, testTime)
		require.NoError(t, err)
		assert.False(t, notLeft)

		_, err = (&pgTx{tx: failing}).MarkParticipantLeft(ctx, testConversation, testFirstUser, testTime)
		require.Error(t, err)

		version, err := (&pgTx{tx: &fakeTx{rowValues: []any{2}}}).BumpConversationVersion(ctx, testConversation, testTime)
		require.NoError(t, err)
		assert.Equal(t, 2, version)

		_, err = (&pgTx{tx: failing}).BumpConversationVersion(ctx, testConversation, testTime)
		require.Error(t, err)

		renamedVersion, err := (&pgTx{tx: &fakeTx{rowValues: []any{3}}}).UpdateConversationName(ctx, testConversation, "New", testTime)
		require.NoError(t, err)
		assert.Equal(t, 3, renamedVersion)

		_, err = (&pgTx{tx: failing}).UpdateConversationName(ctx, testConversation, "New", testTime)
		require.Error(t, err)
	})
}

// participantsAwareTx answers the first Query with the scripted conversation
// rows and every later Query with an empty participant set.
type participantsAwareTx struct {
	first *fakeTx
	calls int
}

func (p *participantsAwareTx) Begin(context.Context) (pgx.Tx, error) { return p, nil }
func (p *participantsAwareTx) Commit(context.Context) error          { return nil }
func (p *participantsAwareTx) Rollback(context.Context) error        { return nil }
func (p *participantsAwareTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (p *participantsAwareTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (p *participantsAwareTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (p *participantsAwareTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (p *participantsAwareTx) Conn() *pgx.Conn { return nil }
func (p *participantsAwareTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.first.Exec(ctx, sql, args...)
}
func (p *participantsAwareTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.calls++
	if p.calls > 1 {
		return newFakeRows(), nil
	}
	return p.first.Query(ctx, sql, args...)
}
func (p *participantsAwareTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.first.QueryRow(ctx, sql, args...)
}

func TestPgTxOutboxAndIdempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	record := models.OutboxRecord{
		ID:            uuid.New(),
		AggregateType: models.AggregateTypeConversation,
		AggregateID:   testConversation,
		EventType:     "conversation_changed",
		EventVersion:  1,
		Topic:         "social_events",
		PartitionKey:  testConversation.String(),
		CorrelationID: uuid.New(),
		TraceID:       "0123456789abcdef0123456789abcdef",
		Payload:       []byte(`{"conversation_id":"` + testConversation.String() + `"}`),
		CreatedAt:     testTime,
	}

	tx := &fakeTx{execTag: pgconn.NewCommandTag("INSERT 0 1")}
	require.NoError(t, (&pgTx{tx: tx}).InsertOutboxRecord(ctx, record))
	assert.Contains(t, tx.execSQL[0], "INSERT INTO event_outbox")
	assert.Len(t, tx.execArgs[0], 13)

	causationID := uuid.New()
	listTx := &fakeTx{queryRows: newFakeRows([]any{
		record.ID, record.AggregateType, record.AggregateID, record.EventType, record.EventVersion,
		record.Topic, record.PartitionKey, record.CorrelationID, causationID, record.TraceID,
		record.Payload, nil, testTime,
	})}
	records, err := (&pgTx{tx: listTx}).ListUnpublishedOutbox(ctx, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, record.ID, records[0].ID)
	require.NotNil(t, records[0].CausationID)
	assert.Equal(t, causationID, *records[0].CausationID)
	assert.Nil(t, records[0].PublishedAt)
	assert.Contains(t, listTx.querySQL, "WHERE published_at IS NULL")

	_, err = (&pgTx{tx: &fakeTx{queryErr: errors.New("boom")}}).ListUnpublishedOutbox(ctx, 10)
	require.Error(t, err)
	_, err = (&pgTx{tx: &fakeTx{queryRows: &fakeRows{err: errors.New("boom")}}}).ListUnpublishedOutbox(ctx, 10)
	require.Error(t, err)

	markTx := &fakeTx{execTag: pgconn.NewCommandTag("UPDATE 1")}
	require.NoError(t, (&pgTx{tx: markTx}).MarkOutboxPublished(ctx, []uuid.UUID{record.ID, uuid.New()}, testTime))
	assert.Len(t, markTx.execSQL, 2)
	assert.Contains(t, markTx.execSQL[0], "UPDATE event_outbox SET published_at")

	countTx := &fakeTx{rowValues: []any{int64(2)}}
	count, err := (&pgTx{tx: countTx}).CountUnpublishedOutbox(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
	_, err = (&pgTx{tx: &fakeTx{rowErr: errors.New("boom")}}).CountUnpublishedOutbox(ctx)
	require.Error(t, err)

	previousTx := &fakeTx{rowValues: []any{record.ID}}
	previous, err := (&pgTx{tx: previousTx}).PreviousOutboxEventID(ctx, models.AggregateTypeConversation, testConversation)
	require.NoError(t, err)
	require.NotNil(t, previous)
	assert.Equal(t, record.ID, *previous)

	_, err = (&pgTx{tx: &fakeTx{rowErr: pgx.ErrNoRows}}).PreviousOutboxEventID(ctx, models.AggregateTypeConversation, testConversation)
	assert.ErrorIs(t, err, ErrNoRows)

	idempotencyTx := &fakeTx{rowValues: []any{testFirstUser, "POST /friend-requests", "key", "hash", 201, record.ID, testTime}}
	stored, err := (&pgTx{tx: idempotencyTx}).GetIdempotencyRecord(ctx, testFirstUser, "POST /friend-requests", "key")
	require.NoError(t, err)
	assert.Equal(t, 201, stored.StatusCode)
	assert.Equal(t, record.ID, stored.ResourceID)

	_, err = (&pgTx{tx: &fakeTx{rowErr: pgx.ErrNoRows}}).GetIdempotencyRecord(ctx, testFirstUser, "endpoint", "key")
	assert.ErrorIs(t, err, ErrNoRows)

	insertTx := &fakeTx{execTag: pgconn.NewCommandTag("INSERT 0 1")}
	require.NoError(t, (&pgTx{tx: insertTx}).InsertIdempotencyRecord(ctx, models.IdempotencyRecord{
		UserID: testFirstUser, Endpoint: "endpoint", Key: "key", RequestHash: "hash",
		StatusCode: 201, ResourceID: record.ID, CreatedAt: testTime,
	}))
	assert.Contains(t, insertTx.execSQL[0], "INSERT INTO idempotency_record")

	err = (&pgTx{tx: &fakeTx{execErr: &pgconn.PgError{Code: "23505", ConstraintName: "idempotency_record_pkey"}}}).
		InsertIdempotencyRecord(ctx, models.IdempotencyRecord{UserID: testFirstUser, Endpoint: "endpoint", Key: "key"})
	assert.ErrorIs(t, err, ErrUniqueViolation)
}

func TestTranslateError(t *testing.T) {
	t.Parallel()

	assert.NoError(t, translateError(nil))
	assert.ErrorIs(t, translateError(pgx.ErrNoRows), ErrNoRows)
	assert.ErrorIs(t, translateError(&pgconn.PgError{Code: "23505", ConstraintName: "uq_direct_pair"}), ErrUniqueViolation)
	assert.ErrorIs(t, translateError(&pgconn.PgError{Code: "23514", ConstraintName: "conversation_name_length"}), ErrCheckViolation)
	assert.ErrorIs(t, translateError(&pgconn.PgError{Code: "23503", ConstraintName: "conversation_participant_conversation_id_fkey"}), ErrCheckViolation)
	assert.ErrorIs(t, translateError(context.DeadlineExceeded), ErrUnavailable)

	other := errors.New("some other failure")
	assert.Equal(t, other, translateError(other))
}

func TestPaginationHelpers(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 51, PaginationFetchSize(50))

	rows := []models.FriendRequest{
		{ID: uuid.New(), CreatedAt: testTime},
		{ID: uuid.New(), CreatedAt: testTime},
		{ID: uuid.New(), CreatedAt: testTime},
	}
	next := NextCursor(rows, 2, func(request models.FriendRequest) models.Cursor {
		return models.Cursor{CreatedAt: request.CreatedAt, ID: request.ID}
	})
	require.NotNil(t, next)
	decoded, ok, err := models.ParseCursor(*next)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, rows[1].ID, decoded.ID)

	assert.Nil(t, NextCursor(rows[:2], 2, func(request models.FriendRequest) models.Cursor {
		return models.Cursor{CreatedAt: request.CreatedAt, ID: request.ID}
	}), "a short page has no next cursor")
}

func TestMemoryStoreTransactionSemantics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryStore()
	t.Cleanup(store.Close)

	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		return tx.InsertFriendship(ctx, models.Friendship{UserIDA: testFirstUser, UserIDB: testSecondUser, CreatedAt: testTime})
	}))

	rolledBack := errors.New("rollback")
	err := store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		if _, err := tx.DeleteFriendship(ctx, testFirstUser, testSecondUser); err != nil {
			return err
		}
		return rolledBack
	})
	assert.ErrorIs(t, err, rolledBack)

	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		_, err := tx.GetFriendship(ctx, testFirstUser, testSecondUser)
		return err
	}))

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	assert.ErrorIs(t, store.InTx(cancelledCtx, func(context.Context, Tx) error { return nil }), context.Canceled)

	store.SetPingError(ErrUnavailable)
	assert.ErrorIs(t, store.Ping(ctx), ErrUnavailable)
	store.SetPingError(nil)
	require.NoError(t, store.Ping(ctx))
	assert.Positive(t, store.TxCount())

	store.Close()
	assert.ErrorIs(t, store.Ping(ctx), ErrUnavailable)
	assert.ErrorIs(t, store.InTx(ctx, func(context.Context, Tx) error { return nil }), ErrUnavailable)
}
