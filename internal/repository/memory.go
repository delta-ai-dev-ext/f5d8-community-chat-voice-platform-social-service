package repository

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"social_service/internal/models"
)

// MemoryStore is an in-memory implementation of Store that emulates the
// storage-level constraints of architecture §5: one pending request per
// canonical pair, one friendship row per canonical pair, one direct
// conversation per pair key, one participant row per (conversation, user), and
// outbox ordering. It is used by the unit and handler tests (and local
// development) so no PostgreSQL or Docker is required.
type MemoryStore struct {
	mu        sync.Mutex
	state     *memoryState
	failPing  error
	inTxCount int
}

type memoryState struct {
	friendRequests map[uuid.UUID]*models.FriendRequest
	friendships    map[string]*models.Friendship
	conversations  map[uuid.UUID]*models.Conversation
	outbox         []*models.OutboxRecord
	idempotency    map[string]*models.IdempotencyRecord
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: newMemoryState()}
}

func newMemoryState() *memoryState {
	return &memoryState{
		friendRequests: map[uuid.UUID]*models.FriendRequest{},
		friendships:    map[string]*models.Friendship{},
		conversations:  map[uuid.UUID]*models.Conversation{},
		outbox:         []*models.OutboxRecord{},
		idempotency:    map[string]*models.IdempotencyRecord{},
	}
}

func (s *memoryState) clone() *memoryState {
	clone := newMemoryState()
	for id, request := range s.friendRequests {
		copied := *request
		clone.friendRequests[id] = &copied
	}
	for key, friendship := range s.friendships {
		copied := *friendship
		clone.friendships[key] = &copied
	}
	for id, conversation := range s.conversations {
		clone.conversations[id] = cloneConversation(conversation)
	}
	clone.outbox = make([]*models.OutboxRecord, 0, len(s.outbox))
	for _, record := range s.outbox {
		copied := *record
		if record.PublishedAt != nil {
			publishedAt := *record.PublishedAt
			copied.PublishedAt = &publishedAt
		}
		if record.CausationID != nil {
			causationID := *record.CausationID
			copied.CausationID = &causationID
		}
		copied.Payload = append([]byte(nil), record.Payload...)
		clone.outbox = append(clone.outbox, &copied)
	}
	for key, record := range s.idempotency {
		copied := *record
		clone.idempotency[key] = &copied
	}
	return clone
}

func cloneConversation(conversation *models.Conversation) *models.Conversation {
	copied := *conversation
	copied.Participants = make([]models.Participant, 0, len(conversation.Participants))
	for _, participant := range conversation.Participants {
		participantCopy := participant
		if participant.LeftAt != nil {
			leftAt := *participant.LeftAt
			participantCopy.LeftAt = &leftAt
		}
		copied.Participants = append(copied.Participants, participantCopy)
	}
	if conversation.Name != nil {
		name := *conversation.Name
		copied.Name = &name
	}
	if conversation.DirectPairKey != nil {
		pairKey := *conversation.DirectPairKey
		copied.DirectPairKey = &pairKey
	}
	return &copied
}

// InTx runs fn against a copy of the state and swaps it in on success, giving
// the same all-or-nothing semantics as a PostgreSQL transaction.
func (s *MemoryStore) InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.inTxCount++
	working := s.state.clone()
	tx := &memoryTx{state: working}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	s.state = working
	return nil
}

// Ping reports store availability.
func (s *MemoryStore) Ping(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.failPing != nil {
		return s.failPing
	}
	if s.state == nil {
		return ErrUnavailable
	}
	return nil
}

// SetPingError makes Ping fail, to exercise readiness degradation.
func (s *MemoryStore) SetPingError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failPing = err
}

// Close releases the store.
func (s *MemoryStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = nil
}

// TxCount reports how many transactions were started (used by tests to assert
// that a rule violation wrote nothing).
func (s *MemoryStore) TxCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inTxCount
}

type memoryTx struct {
	state *memoryState
}

func (t *memoryTx) InsertFriendRequest(_ context.Context, request models.FriendRequest) error {
	if request.SenderUserID == request.RecipientUserID {
		return fmt.Errorf("%w: check sender_user_id <> recipient_user_id", ErrCheckViolation)
	}
	pairKey := models.PairKey(request.SenderUserID, request.RecipientUserID)
	for _, existing := range t.state.friendRequests {
		if existing.Status != models.FriendRequestPending {
			continue
		}
		if models.PairKey(existing.SenderUserID, existing.RecipientUserID) == pairKey {
			return fmt.Errorf("%w: uq_friend_request_pending_pair", ErrUniqueViolation)
		}
	}
	stored := request
	t.state.friendRequests[request.ID] = &stored
	return nil
}

func (t *memoryTx) GetFriendRequest(_ context.Context, id uuid.UUID) (*models.FriendRequest, error) {
	request, ok := t.state.friendRequests[id]
	if !ok {
		return nil, ErrNoRows
	}
	copied := *request
	return &copied, nil
}

func (t *memoryTx) LockFriendRequest(ctx context.Context, id uuid.UUID) (*models.FriendRequest, error) {
	return t.GetFriendRequest(ctx, id)
}

func (t *memoryTx) UpdateFriendRequestStatus(_ context.Context, id uuid.UUID, status models.FriendRequestStatus, at time.Time) error {
	request, ok := t.state.friendRequests[id]
	if !ok {
		return ErrNoRows
	}
	request.Status = status
	request.UpdatedAt = at.UTC()
	return nil
}

func (t *memoryTx) PendingFriendRequestBetween(_ context.Context, first, second uuid.UUID) (*models.FriendRequest, error) {
	pairKey := models.PairKey(first, second)
	for _, request := range t.state.friendRequests {
		if request.Status != models.FriendRequestPending {
			continue
		}
		if models.PairKey(request.SenderUserID, request.RecipientUserID) == pairKey {
			copied := *request
			return &copied, nil
		}
	}
	return nil, ErrNoRows
}

func (t *memoryTx) ListFriendRequests(_ context.Context, userID uuid.UUID, direction models.RequestDirection, cursor *models.Cursor, limit int) ([]models.FriendRequest, error) {
	rows := make([]models.FriendRequest, 0, len(t.state.friendRequests))
	for _, request := range t.state.friendRequests {
		if request.Status != models.FriendRequestPending {
			continue
		}
		if direction == models.DirectionOutgoing {
			if request.SenderUserID != userID {
				continue
			}
		} else if request.RecipientUserID != userID {
			continue
		}
		if !afterCursor(request.CreatedAt, request.ID, cursor) {
			continue
		}
		rows = append(rows, *request)
	}
	sortFriendRequests(rows)
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (t *memoryTx) InsertFriendship(_ context.Context, friendship models.Friendship) error {
	lesser, greater := models.CanonicalPair(friendship.UserIDA, friendship.UserIDB)
	key := models.PairKey(lesser, greater)
	if _, exists := t.state.friendships[key]; exists {
		return fmt.Errorf("%w: friendship_pkey", ErrUniqueViolation)
	}
	stored := models.Friendship{UserIDA: lesser, UserIDB: greater, CreatedAt: friendship.CreatedAt.UTC()}
	t.state.friendships[key] = &stored
	return nil
}

func (t *memoryTx) GetFriendship(_ context.Context, first, second uuid.UUID) (*models.Friendship, error) {
	friendship, ok := t.state.friendships[models.PairKey(first, second)]
	if !ok {
		return nil, ErrNoRows
	}
	copied := *friendship
	return &copied, nil
}

func (t *memoryTx) DeleteFriendship(_ context.Context, first, second uuid.UUID) (bool, error) {
	key := models.PairKey(first, second)
	if _, ok := t.state.friendships[key]; !ok {
		return false, nil
	}
	delete(t.state.friendships, key)
	return true, nil
}

func (t *memoryTx) ListFriendships(_ context.Context, userID uuid.UUID, cursor *models.Cursor, limit int) ([]models.Friendship, error) {
	rows := make([]models.Friendship, 0, len(t.state.friendships))
	for _, friendship := range t.state.friendships {
		if friendship.UserIDA != userID && friendship.UserIDB != userID {
			continue
		}
		other, ok := friendship.Other(userID)
		if !ok {
			continue
		}
		if !afterCursor(friendship.CreatedAt, other, cursor) {
			continue
		}
		rows = append(rows, *friendship)
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		otherI, _ := rows[i].Other(userID)
		otherJ, _ := rows[j].Other(userID)
		return otherI.String() > otherJ.String()
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (t *memoryTx) CountFriendships(context.Context) (int64, error) {
	return int64(len(t.state.friendships)), nil
}

func (t *memoryTx) InsertConversation(_ context.Context, conversation models.Conversation) error {
	if _, exists := t.state.conversations[conversation.ID]; exists {
		return fmt.Errorf("%w: conversation_pkey", ErrUniqueViolation)
	}
	if conversation.Kind == models.ConversationDirect && conversation.DirectPairKey != nil {
		for _, existing := range t.state.conversations {
			if existing.Kind == models.ConversationDirect && existing.DirectPairKey != nil && *existing.DirectPairKey == *conversation.DirectPairKey {
				return fmt.Errorf("%w: uq_direct_pair", ErrUniqueViolation)
			}
		}
	}
	stored := cloneConversation(&conversation)
	t.state.conversations[conversation.ID] = stored
	return nil
}

func (t *memoryTx) GetConversation(_ context.Context, id uuid.UUID) (*models.Conversation, error) {
	conversation, ok := t.state.conversations[id]
	if !ok {
		return nil, ErrNoRows
	}
	return cloneConversation(conversation), nil
}

func (t *memoryTx) DirectConversationByPairKey(_ context.Context, pairKey string) (*models.Conversation, error) {
	for _, conversation := range t.state.conversations {
		if conversation.Kind != models.ConversationDirect || conversation.DirectPairKey == nil {
			continue
		}
		if *conversation.DirectPairKey == pairKey {
			return cloneConversation(conversation), nil
		}
	}
	return nil, ErrNoRows
}

func (t *memoryTx) ListConversations(_ context.Context, userID uuid.UUID, cursor *models.Cursor, limit int) ([]models.Conversation, error) {
	rows := make([]models.Conversation, 0, len(t.state.conversations))
	for _, conversation := range t.state.conversations {
		if !conversation.IsActiveParticipant(userID) {
			continue
		}
		if !afterCursor(conversation.CreatedAt, conversation.ID, cursor) {
			continue
		}
		rows = append(rows, *cloneConversation(conversation))
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		return rows[i].ID.String() > rows[j].ID.String()
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (t *memoryTx) UpsertParticipant(_ context.Context, participant models.Participant) (bool, error) {
	conversation, ok := t.state.conversations[participant.ConversationID]
	if !ok {
		return false, ErrNoRows
	}
	for i := range conversation.Participants {
		if conversation.Participants[i].UserID != participant.UserID {
			continue
		}
		if conversation.Participants[i].Active() {
			return false, nil
		}
		conversation.Participants[i].LeftAt = nil
		conversation.Participants[i].JoinedAt = participant.JoinedAt.UTC()
		return true, nil
	}
	joinedAt := participant.JoinedAt.UTC()
	conversation.Participants = append(conversation.Participants, models.Participant{
		ConversationID: participant.ConversationID,
		UserID:         participant.UserID,
		JoinedAt:       joinedAt,
	})
	sortParticipants(conversation.Participants)
	return true, nil
}

func (t *memoryTx) MarkParticipantLeft(_ context.Context, conversationID, userID uuid.UUID, at time.Time) (bool, error) {
	conversation, ok := t.state.conversations[conversationID]
	if !ok {
		return false, ErrNoRows
	}
	for i := range conversation.Participants {
		if conversation.Participants[i].UserID != userID || !conversation.Participants[i].Active() {
			continue
		}
		leftAt := at.UTC()
		conversation.Participants[i].LeftAt = &leftAt
		return true, nil
	}
	return false, nil
}

func (t *memoryTx) BumpConversationVersion(_ context.Context, conversationID uuid.UUID, at time.Time) (int, error) {
	conversation, ok := t.state.conversations[conversationID]
	if !ok {
		return 0, ErrNoRows
	}
	conversation.Version++
	conversation.UpdatedAt = at.UTC()
	return conversation.Version, nil
}

func (t *memoryTx) UpdateConversationName(_ context.Context, conversationID uuid.UUID, name string, at time.Time) (int, error) {
	conversation, ok := t.state.conversations[conversationID]
	if !ok {
		return 0, ErrNoRows
	}
	trimmed := name
	conversation.Name = &trimmed
	conversation.Version++
	conversation.UpdatedAt = at.UTC()
	return conversation.Version, nil
}

func (t *memoryTx) InsertOutboxRecord(_ context.Context, record models.OutboxRecord) error {
	stored := record
	stored.Payload = append([]byte(nil), record.Payload...)
	t.state.outbox = append(t.state.outbox, &stored)
	return nil
}

func (t *memoryTx) ListUnpublishedOutbox(_ context.Context, limit int) ([]models.OutboxRecord, error) {
	rows := make([]models.OutboxRecord, 0, len(t.state.outbox))
	for _, record := range t.state.outbox {
		if record.PublishedAt != nil {
			continue
		}
		rows = append(rows, *record)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.Before(rows[j].CreatedAt)
		}
		return rows[i].ID.String() < rows[j].ID.String()
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (t *memoryTx) MarkOutboxPublished(_ context.Context, ids []uuid.UUID, at time.Time) error {
	targets := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		targets[id] = struct{}{}
	}
	for _, record := range t.state.outbox {
		if _, ok := targets[record.ID]; !ok {
			continue
		}
		publishedAt := at.UTC()
		record.PublishedAt = &publishedAt
	}
	return nil
}

func (t *memoryTx) CountUnpublishedOutbox(context.Context) (int64, error) {
	var count int64
	for _, record := range t.state.outbox {
		if record.PublishedAt == nil {
			count++
		}
	}
	return count, nil
}

func (t *memoryTx) PreviousOutboxEventID(_ context.Context, aggregateType string, aggregateID uuid.UUID) (*uuid.UUID, error) {
	var latest *models.OutboxRecord
	for _, record := range t.state.outbox {
		if record.AggregateType != aggregateType || record.AggregateID != aggregateID {
			continue
		}
		if latest == nil || record.CreatedAt.After(latest.CreatedAt) {
			latest = record
		}
	}
	if latest == nil {
		return nil, ErrNoRows
	}
	id := latest.ID
	return &id, nil
}

func (t *memoryTx) GetIdempotencyRecord(_ context.Context, userID uuid.UUID, endpoint, key string) (*models.IdempotencyRecord, error) {
	record, ok := t.state.idempotency[idempotencyMapKey(userID, endpoint, key)]
	if !ok {
		return nil, ErrNoRows
	}
	copied := *record
	return &copied, nil
}

func (t *memoryTx) InsertIdempotencyRecord(_ context.Context, record models.IdempotencyRecord) error {
	key := idempotencyMapKey(record.UserID, record.Endpoint, record.Key)
	if _, exists := t.state.idempotency[key]; exists {
		return fmt.Errorf("%w: idempotency_record_pkey", ErrUniqueViolation)
	}
	stored := record
	t.state.idempotency[key] = &stored
	return nil
}

func idempotencyMapKey(userID uuid.UUID, endpoint, key string) string {
	return userID.String() + "|" + endpoint + "|" + key
}

// afterCursor reports whether a row at (createdAt, id) sorts strictly after the
// cursor in (created_at DESC, id DESC) order, i.e. whether it belongs to a
// later page.
func afterCursor(createdAt time.Time, id uuid.UUID, cursor *models.Cursor) bool {
	if cursor == nil {
		return true
	}
	if createdAt.Before(cursor.CreatedAt) {
		return true
	}
	if createdAt.Equal(cursor.CreatedAt) {
		return id.String() < cursor.ID.String()
	}
	return false
}

func sortFriendRequests(rows []models.FriendRequest) {
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		return rows[i].ID.String() > rows[j].ID.String()
	})
}

func sortParticipants(participants []models.Participant) {
	sort.SliceStable(participants, func(i, j int) bool {
		if !participants[i].JoinedAt.Equal(participants[j].JoinedAt) {
			return participants[i].JoinedAt.Before(participants[j].JoinedAt)
		}
		return participants[i].UserID.String() < participants[j].UserID.String()
	})
}
