package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"social_service/internal/models"
)

const friendRequestColumns = `id, sender_user_id, recipient_user_id, status, created_at, updated_at`

const conversationColumns = `c.id, c.kind, c.name, c.created_by, c.direct_pair_key, c.conversation_version, c.created_at, c.updated_at`

const outboxColumns = `id, aggregate_type, aggregate_id, event_type, event_version, topic, partition_key, correlation_id, causation_id, trace_id, payload, published_at, created_at`

// PostgresStore is the pgx/pgxpool-backed Store. All statements are
// parameterized; no SQL is assembled from user input.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore opens a bounded connection pool.
func NewPostgresStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse database url: %w", err)
	}
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = time.Hour
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("postgres: open pool: %w", translateError(err))
	}
	return &PostgresStore{pool: pool}, nil
}

// InTx runs fn inside one transaction; a nil error commits.
func (s *PostgresStore) InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return translateError(err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	if err := fn(ctx, &pgTx{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return translateError(err)
	}
	return nil
}

// Ping checks the pool, used by /readyz.
func (s *PostgresStore) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return translateError(err)
	}
	return nil
}

// Close releases the pool.
func (s *PostgresStore) Close() {
	s.pool.Close()
}

type pgTx struct {
	tx pgx.Tx
}

func (t *pgTx) InsertFriendRequest(ctx context.Context, request models.FriendRequest) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO friend_request (`+friendRequestColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		request.ID, request.SenderUserID, request.RecipientUserID, string(request.Status),
		request.CreatedAt, request.UpdatedAt)
	return translateError(err)
}

func (t *pgTx) GetFriendRequest(ctx context.Context, id uuid.UUID) (*models.FriendRequest, error) {
	return scanFriendRequest(t.tx.QueryRow(ctx, `SELECT `+friendRequestColumns+` FROM friend_request WHERE id = $1`, id))
}

func (t *pgTx) LockFriendRequest(ctx context.Context, id uuid.UUID) (*models.FriendRequest, error) {
	return scanFriendRequest(t.tx.QueryRow(ctx, `SELECT `+friendRequestColumns+` FROM friend_request WHERE id = $1 FOR UPDATE`, id))
}

func (t *pgTx) UpdateFriendRequestStatus(ctx context.Context, id uuid.UUID, status models.FriendRequestStatus, at time.Time) error {
	tag, err := t.tx.Exec(ctx, `UPDATE friend_request SET status = $2, updated_at = $3 WHERE id = $1`, id, string(status), at)
	if err != nil {
		return translateError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNoRows
	}
	return nil
}

func (t *pgTx) PendingFriendRequestBetween(ctx context.Context, first, second uuid.UUID) (*models.FriendRequest, error) {
	return scanFriendRequest(t.tx.QueryRow(ctx, `SELECT `+friendRequestColumns+`
		FROM friend_request
		WHERE status = 'pending'
		  AND ((sender_user_id = $1 AND recipient_user_id = $2) OR (sender_user_id = $2 AND recipient_user_id = $1))
		LIMIT 1`, first, second))
}

func (t *pgTx) ListFriendRequests(ctx context.Context, userID uuid.UUID, direction models.RequestDirection, cursor *models.Cursor, limit int) ([]models.FriendRequest, error) {
	column := "recipient_user_id"
	if direction == models.DirectionOutgoing {
		column = "sender_user_id"
	}
	query := `SELECT ` + friendRequestColumns + ` FROM friend_request WHERE status = 'pending' AND ` + column + ` = $1`
	args := []any{userID}
	if cursor != nil {
		query += ` AND (created_at, id) < ($2, $3)`
		args = append(args, cursor.CreatedAt, cursor.ID)
	}
	args = append(args, limit)
	query += ` ORDER BY created_at DESC, id DESC LIMIT $` + strconv.Itoa(len(args))

	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, translateError(err)
	}
	defer rows.Close()

	requests := make([]models.FriendRequest, 0, limit)
	for rows.Next() {
		var request models.FriendRequest
		if err := rows.Scan(&request.ID, &request.SenderUserID, &request.RecipientUserID, &request.Status, &request.CreatedAt, &request.UpdatedAt); err != nil {
			return nil, translateError(err)
		}
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err)
	}
	return requests, nil
}

func (t *pgTx) InsertFriendship(ctx context.Context, friendship models.Friendship) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO friendship (user_id_a, user_id_b, created_at) VALUES ($1, $2, $3)`,
		friendship.UserIDA, friendship.UserIDB, friendship.CreatedAt)
	return translateError(err)
}

func (t *pgTx) GetFriendship(ctx context.Context, first, second uuid.UUID) (*models.Friendship, error) {
	lesser, greater := models.CanonicalPair(first, second)
	var friendship models.Friendship
	err := t.tx.QueryRow(ctx, `SELECT user_id_a, user_id_b, created_at FROM friendship WHERE user_id_a = $1 AND user_id_b = $2`,
		lesser, greater).Scan(&friendship.UserIDA, &friendship.UserIDB, &friendship.CreatedAt)
	if err != nil {
		return nil, translateError(err)
	}
	return &friendship, nil
}

func (t *pgTx) DeleteFriendship(ctx context.Context, first, second uuid.UUID) (bool, error) {
	lesser, greater := models.CanonicalPair(first, second)
	tag, err := t.tx.Exec(ctx, `DELETE FROM friendship WHERE user_id_a = $1 AND user_id_b = $2`, lesser, greater)
	if err != nil {
		return false, translateError(err)
	}
	return tag.RowsAffected() > 0, nil
}

func (t *pgTx) ListFriendships(ctx context.Context, userID uuid.UUID, cursor *models.Cursor, limit int) ([]models.Friendship, error) {
	otherExpr := `CASE WHEN user_id_a = $1 THEN user_id_b ELSE user_id_a END`
	query := `SELECT user_id_a, user_id_b, created_at FROM friendship WHERE (user_id_a = $1 OR user_id_b = $1)`
	args := []any{userID}
	if cursor != nil {
		query += ` AND (created_at, ` + otherExpr + `) < ($2, $3)`
		args = append(args, cursor.CreatedAt, cursor.ID)
	}
	args = append(args, limit)
	query += ` ORDER BY created_at DESC, ` + otherExpr + ` DESC LIMIT $` + strconv.Itoa(len(args))

	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, translateError(err)
	}
	defer rows.Close()

	friendships := make([]models.Friendship, 0, limit)
	for rows.Next() {
		var friendship models.Friendship
		if err := rows.Scan(&friendship.UserIDA, &friendship.UserIDB, &friendship.CreatedAt); err != nil {
			return nil, translateError(err)
		}
		friendships = append(friendships, friendship)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err)
	}
	return friendships, nil
}

func (t *pgTx) CountFriendships(ctx context.Context) (int64, error) {
	var count int64
	if err := t.tx.QueryRow(ctx, `SELECT count(*) FROM friendship`).Scan(&count); err != nil {
		return 0, translateError(err)
	}
	return count, nil
}

func (t *pgTx) InsertConversation(ctx context.Context, conversation models.Conversation) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO conversation
			(id, kind, name, created_by, direct_pair_key, conversation_version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		conversation.ID, string(conversation.Kind), conversation.Name, conversation.CreatedBy,
		conversation.DirectPairKey, conversation.Version, conversation.CreatedAt, conversation.UpdatedAt)
	if err != nil {
		return translateError(err)
	}
	for _, participant := range conversation.Participants {
		if _, err := t.tx.Exec(ctx, `INSERT INTO conversation_participant (conversation_id, user_id, joined_at, left_at)
			VALUES ($1, $2, $3, $4)`,
			participant.ConversationID, participant.UserID, participant.JoinedAt, participant.LeftAt); err != nil {
			return translateError(err)
		}
	}
	return nil
}

func (t *pgTx) GetConversation(ctx context.Context, id uuid.UUID) (*models.Conversation, error) {
	conversation, err := scanConversation(t.tx.QueryRow(ctx, `SELECT `+conversationColumns+` FROM conversation c WHERE c.id = $1`, id))
	if err != nil {
		return nil, err
	}
	participants, err := t.participants(ctx, []uuid.UUID{conversation.ID})
	if err != nil {
		return nil, err
	}
	conversation.Participants = participants[conversation.ID]
	return conversation, nil
}

func (t *pgTx) DirectConversationByPairKey(ctx context.Context, pairKey string) (*models.Conversation, error) {
	conversation, err := scanConversation(t.tx.QueryRow(ctx, `SELECT `+conversationColumns+`
		FROM conversation c
		WHERE c.kind = 'direct' AND c.direct_pair_key = $1`, pairKey))
	if err != nil {
		return nil, err
	}
	participants, err := t.participants(ctx, []uuid.UUID{conversation.ID})
	if err != nil {
		return nil, err
	}
	conversation.Participants = participants[conversation.ID]
	return conversation, nil
}

func (t *pgTx) ListConversations(ctx context.Context, userID uuid.UUID, cursor *models.Cursor, limit int) ([]models.Conversation, error) {
	query := `SELECT ` + conversationColumns + `
		FROM conversation c
		JOIN conversation_participant p ON p.conversation_id = c.id
		WHERE p.user_id = $1 AND p.left_at IS NULL`
	args := []any{userID}
	if cursor != nil {
		query += ` AND (c.created_at, c.id) < ($2, $3)`
		args = append(args, cursor.CreatedAt, cursor.ID)
	}
	args = append(args, limit)
	query += ` ORDER BY c.created_at DESC, c.id DESC LIMIT $` + strconv.Itoa(len(args))

	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, translateError(err)
	}
	conversations := make([]models.Conversation, 0, limit)
	ids := make([]uuid.UUID, 0, limit)
	for rows.Next() {
		conversation, err := scanConversationRow(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		conversations = append(conversations, *conversation)
		ids = append(ids, conversation.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, translateError(err)
	}
	if len(ids) == 0 {
		return conversations, nil
	}
	participants, err := t.participants(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i := range conversations {
		conversations[i].Participants = participants[conversations[i].ID]
	}
	return conversations, nil
}

func (t *pgTx) UpsertParticipant(ctx context.Context, participant models.Participant) (bool, error) {
	tag, err := t.tx.Exec(ctx, `INSERT INTO conversation_participant (conversation_id, user_id, joined_at, left_at)
		VALUES ($1, $2, $3, NULL)
		ON CONFLICT (conversation_id, user_id) DO UPDATE
		SET left_at = NULL, joined_at = EXCLUDED.joined_at
		WHERE conversation_participant.left_at IS NOT NULL`,
		participant.ConversationID, participant.UserID, participant.JoinedAt)
	if err != nil {
		return false, translateError(err)
	}
	return tag.RowsAffected() > 0, nil
}

func (t *pgTx) MarkParticipantLeft(ctx context.Context, conversationID, userID uuid.UUID, at time.Time) (bool, error) {
	tag, err := t.tx.Exec(ctx, `UPDATE conversation_participant SET left_at = $3
		WHERE conversation_id = $1 AND user_id = $2 AND left_at IS NULL`, conversationID, userID, at)
	if err != nil {
		return false, translateError(err)
	}
	return tag.RowsAffected() > 0, nil
}

func (t *pgTx) BumpConversationVersion(ctx context.Context, conversationID uuid.UUID, at time.Time) (int, error) {
	var version int
	err := t.tx.QueryRow(ctx, `UPDATE conversation
		SET conversation_version = conversation_version + 1, updated_at = $2
		WHERE id = $1
		RETURNING conversation_version`, conversationID, at).Scan(&version)
	if err != nil {
		return 0, translateError(err)
	}
	return version, nil
}

func (t *pgTx) UpdateConversationName(ctx context.Context, conversationID uuid.UUID, name string, at time.Time) (int, error) {
	var version int
	err := t.tx.QueryRow(ctx, `UPDATE conversation
		SET name = $2, conversation_version = conversation_version + 1, updated_at = $3
		WHERE id = $1
		RETURNING conversation_version`, conversationID, name, at).Scan(&version)
	if err != nil {
		return 0, translateError(err)
	}
	return version, nil
}

func (t *pgTx) InsertOutboxRecord(ctx context.Context, record models.OutboxRecord) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO event_outbox (`+outboxColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		record.ID, record.AggregateType, record.AggregateID, record.EventType, record.EventVersion,
		record.Topic, record.PartitionKey, record.CorrelationID, record.CausationID, record.TraceID,
		string(record.Payload), record.PublishedAt, record.CreatedAt)
	return translateError(err)
}

func (t *pgTx) ListUnpublishedOutbox(ctx context.Context, limit int) ([]models.OutboxRecord, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+outboxColumns+` FROM event_outbox
		WHERE published_at IS NULL
		ORDER BY created_at ASC, id ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, translateError(err)
	}
	defer rows.Close()

	records := make([]models.OutboxRecord, 0, limit)
	for rows.Next() {
		var record models.OutboxRecord
		var payload []byte
		if err := rows.Scan(&record.ID, &record.AggregateType, &record.AggregateID, &record.EventType,
			&record.EventVersion, &record.Topic, &record.PartitionKey, &record.CorrelationID,
			&record.CausationID, &record.TraceID, &payload, &record.PublishedAt, &record.CreatedAt); err != nil {
			return nil, translateError(err)
		}
		record.Payload = payload
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err)
	}
	return records, nil
}

func (t *pgTx) MarkOutboxPublished(ctx context.Context, ids []uuid.UUID, at time.Time) error {
	for _, id := range ids {
		if _, err := t.tx.Exec(ctx, `UPDATE event_outbox SET published_at = $2 WHERE id = $1 AND published_at IS NULL`, id, at); err != nil {
			return translateError(err)
		}
	}
	return nil
}

func (t *pgTx) CountUnpublishedOutbox(ctx context.Context) (int64, error) {
	var count int64
	if err := t.tx.QueryRow(ctx, `SELECT count(*) FROM event_outbox WHERE published_at IS NULL`).Scan(&count); err != nil {
		return 0, translateError(err)
	}
	return count, nil
}

func (t *pgTx) PreviousOutboxEventID(ctx context.Context, aggregateType string, aggregateID uuid.UUID) (*uuid.UUID, error) {
	var id uuid.UUID
	err := t.tx.QueryRow(ctx, `SELECT id FROM event_outbox
		WHERE aggregate_type = $1 AND aggregate_id = $2
		ORDER BY created_at DESC, id DESC
		LIMIT 1`, aggregateType, aggregateID).Scan(&id)
	if err != nil {
		return nil, translateError(err)
	}
	return &id, nil
}

func (t *pgTx) GetIdempotencyRecord(ctx context.Context, userID uuid.UUID, endpoint, key string) (*models.IdempotencyRecord, error) {
	var record models.IdempotencyRecord
	err := t.tx.QueryRow(ctx, `SELECT user_id, endpoint, idempotency_key, request_hash, status_code, resource_id, created_at
		FROM idempotency_record
		WHERE user_id = $1 AND endpoint = $2 AND idempotency_key = $3`, userID, endpoint, key).
		Scan(&record.UserID, &record.Endpoint, &record.Key, &record.RequestHash, &record.StatusCode, &record.ResourceID, &record.CreatedAt)
	if err != nil {
		return nil, translateError(err)
	}
	return &record, nil
}

func (t *pgTx) InsertIdempotencyRecord(ctx context.Context, record models.IdempotencyRecord) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO idempotency_record
			(user_id, endpoint, idempotency_key, request_hash, status_code, resource_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		record.UserID, record.Endpoint, record.Key, record.RequestHash, record.StatusCode, record.ResourceID, record.CreatedAt)
	return translateError(err)
}

func (t *pgTx) participants(ctx context.Context, conversationIDs []uuid.UUID) (map[uuid.UUID][]models.Participant, error) {
	rows, err := t.tx.Query(ctx, `SELECT conversation_id, user_id, joined_at, left_at
		FROM conversation_participant
		WHERE conversation_id = ANY($1)
		ORDER BY joined_at ASC, user_id ASC`, conversationIDs)
	if err != nil {
		return nil, translateError(err)
	}
	defer rows.Close()

	grouped := make(map[uuid.UUID][]models.Participant, len(conversationIDs))
	for rows.Next() {
		var participant models.Participant
		if err := rows.Scan(&participant.ConversationID, &participant.UserID, &participant.JoinedAt, &participant.LeftAt); err != nil {
			return nil, translateError(err)
		}
		grouped[participant.ConversationID] = append(grouped[participant.ConversationID], participant)
	}
	if err := rows.Err(); err != nil {
		return nil, translateError(err)
	}
	return grouped, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanFriendRequest(row rowScanner) (*models.FriendRequest, error) {
	var request models.FriendRequest
	if err := row.Scan(&request.ID, &request.SenderUserID, &request.RecipientUserID, &request.Status,
		&request.CreatedAt, &request.UpdatedAt); err != nil {
		return nil, translateError(err)
	}
	return &request, nil
}

func scanConversation(row rowScanner) (*models.Conversation, error) {
	return scanConversationRow(row)
}

func scanConversationRow(row rowScanner) (*models.Conversation, error) {
	var conversation models.Conversation
	if err := row.Scan(&conversation.ID, &conversation.Kind, &conversation.Name, &conversation.CreatedBy,
		&conversation.DirectPairKey, &conversation.Version, &conversation.CreatedAt, &conversation.UpdatedAt); err != nil {
		return nil, translateError(err)
	}
	return &conversation, nil
}

// translateError maps driver errors onto the storage sentinels.
func translateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoRows
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return fmt.Errorf("%w: %s", ErrUniqueViolation, pgErr.ConstraintName)
		case "23514": // check_violation
			return fmt.Errorf("%w: %s", ErrCheckViolation, pgErr.ConstraintName)
		case "23503": // foreign_key_violation
			return fmt.Errorf("%w: %s", ErrCheckViolation, pgErr.ConstraintName)
		}
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return err
}
