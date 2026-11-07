// Package conversation implements direct (open-or-reuse) and group
// conversation rules, participant management, version bumping, and the
// conversation events, all inside the same unit of work as the state change.
package conversation

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"social_service/internal/events"
	"social_service/internal/models"
	"social_service/internal/repository"
)

// Idempotency endpoints (the key namespace is per endpoint).
const (
	EndpointOpenDirect  = "POST /conversations/direct"
	EndpointCreateGroup = "POST /conversations/group"
)

// Domain limits from architecture §2.2 / §2.3 and the OpenAPI schema.
const (
	// MaxParticipants is the total participant cap including the creator.
	MaxParticipants = 10
	// MaxGroupFriends is the number of friends a creator may add at creation.
	MaxGroupFriends = MaxParticipants - 1
	// MaxNameLength is the group name limit in characters.
	MaxNameLength = 100
)

// Metrics holds the conversation business metrics from architecture §7.
type Metrics struct {
	// Created is `social_conversations_created_total{kind}`.
	Created *prometheus.CounterVec
	// ParticipantsAdded is `social_conversation_participants_added_total`.
	ParticipantsAdded prometheus.Counter
}

// NewMetrics registers the conversation metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		Created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "social_conversations_created_total",
			Help: "Conversations created (reuse is not counted).",
		}, []string{"kind"}),
		ParticipantsAdded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "social_conversation_participants_added_total",
			Help: "Participants added to existing conversations.",
		}),
	}
	if reg != nil {
		reg.MustRegister(metrics.Created, metrics.ParticipantsAdded)
	}
	for _, kind := range []models.ConversationKind{models.ConversationDirect, models.ConversationGroup} {
		metrics.Created.WithLabelValues(string(kind))
	}
	return metrics
}

// RecordCreated counts one conversation creation.
func (m *Metrics) RecordCreated(kind models.ConversationKind) {
	if m == nil || m.Created == nil {
		return
	}
	m.Created.WithLabelValues(string(kind)).Inc()
}

// RecordParticipantsAdded counts n participant additions.
func (m *Metrics) RecordParticipantsAdded(n int) {
	if m == nil || m.ParticipantsAdded == nil || n <= 0 {
		return
	}
	m.ParticipantsAdded.Add(float64(n))
}

// Service is the conversation domain service.
type Service struct {
	store   repository.Store
	clock   models.Clock
	logger  *zap.Logger
	metrics *Metrics
}

// NewService builds the conversation service.
func NewService(store repository.Store, clock models.Clock, logger *zap.Logger, metrics *Metrics) *Service {
	if clock == nil {
		clock = models.RealClock{}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{store: store, clock: clock, logger: logger, metrics: metrics}
}

// OpenDirectInput describes one open-direct-conversation call.
type OpenDirectInput struct {
	CallerID       uuid.UUID
	FriendID       uuid.UUID
	TraceID        string
	IdempotencyKey *models.IdempotencyKey
}

// OpenDirect returns the existing direct conversation for the pair (200) or
// creates it (201). A reuse publishes no event.
func (s *Service) OpenDirect(ctx context.Context, in OpenDirectInput) (models.Conversation, int, error) {
	if in.CallerID == in.FriendID {
		return models.Conversation{}, 0, models.ErrSelfRequest()
	}

	var (
		result   models.Conversation
		status   = http.StatusCreated
		replayed bool
	)
	err := s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		if in.IdempotencyKey != nil {
			record, err := tx.GetIdempotencyRecord(ctx, in.CallerID, EndpointOpenDirect, in.IdempotencyKey.Key)
			switch {
			case err == nil:
				if record.RequestHash != in.IdempotencyKey.RequestHash {
					return models.ErrIdempotencyKeyReuse()
				}
				existing, err := tx.GetConversation(ctx, record.ResourceID)
				if err != nil {
					return err
				}
				result = *existing
				status = record.StatusCode
				replayed = true
				return nil
			case errors.Is(err, repository.ErrNoRows):
			default:
				return err
			}
		}

		if _, err := tx.GetFriendship(ctx, in.CallerID, in.FriendID); err != nil {
			if errors.Is(err, repository.ErrNoRows) {
				return models.ErrNotFriendsValidation()
			}
			return err
		}

		pairKey := models.PairKey(in.CallerID, in.FriendID)
		existing, err := tx.DirectConversationByPairKey(ctx, pairKey)
		if err == nil {
			result = *existing
			status = http.StatusOK
			return s.recordIdempotency(ctx, tx, in, status, existing.ID)
		}
		if !errors.Is(err, repository.ErrNoRows) {
			return err
		}

		now := s.clock.Now()
		conversationID := uuid.New()
		created := models.Conversation{
			ID:            conversationID,
			Kind:          models.ConversationDirect,
			CreatedBy:     in.CallerID,
			DirectPairKey: &pairKey,
			Version:       1,
			CreatedAt:     now,
			UpdatedAt:     now,
			Participants: []models.Participant{
				{ConversationID: conversationID, UserID: in.CallerID, JoinedAt: now},
				{ConversationID: conversationID, UserID: in.FriendID, JoinedAt: now},
			},
		}
		if err := tx.InsertConversation(ctx, created); err != nil {
			if errors.Is(err, repository.ErrUniqueViolation) {
				// Concurrent open of the same pair: converge on the winner and
				// publish nothing (nothing changed).
				winner, getErr := tx.DirectConversationByPairKey(ctx, pairKey)
				if getErr != nil {
					return getErr
				}
				result = *winner
				status = http.StatusOK
				return s.recordIdempotency(ctx, tx, in, status, winner.ID)
			}
			return err
		}

		record, err := events.NewConversationOutbox(events.ConversationEvent{
			ConversationID: created.ID,
			Kind:           created.Kind,
			Change:         events.ChangeCreated,
			ParticipantIDs: []uuid.UUID{in.CallerID, in.FriendID},
			Version:        created.Version,
			TraceID:        in.TraceID,
			OccurredAt:     now,
		})
		if err != nil {
			return err
		}
		if err := tx.InsertOutboxRecord(ctx, *record); err != nil {
			return err
		}
		if err := s.recordIdempotency(ctx, tx, in, status, created.ID); err != nil {
			return err
		}
		result = created
		return nil
	})
	if err != nil {
		return models.Conversation{}, 0, err
	}
	if !replayed && status == http.StatusCreated {
		s.metrics.RecordCreated(models.ConversationDirect)
	}
	return result, status, nil
}

func (s *Service) recordIdempotency(ctx context.Context, tx repository.Tx, in OpenDirectInput, status int, resourceID uuid.UUID) error {
	if in.IdempotencyKey == nil {
		return nil
	}
	err := tx.InsertIdempotencyRecord(ctx, models.IdempotencyRecord{
		UserID:      in.CallerID,
		Endpoint:    EndpointOpenDirect,
		Key:         in.IdempotencyKey.Key,
		RequestHash: in.IdempotencyKey.RequestHash,
		StatusCode:  status,
		ResourceID:  resourceID,
		CreatedAt:   s.clock.Now(),
	})
	if errors.Is(err, repository.ErrUniqueViolation) {
		return models.ErrIdempotencyKeyReuse()
	}
	return err
}

// CreateGroupInput describes one create-group-conversation call.
type CreateGroupInput struct {
	CallerID       uuid.UUID
	Name           *string
	ParticipantIDs []uuid.UUID
	TraceID        string
	IdempotencyKey *models.IdempotencyKey
}

// CreateGroup creates a group conversation with the caller plus 1..9 friends,
// all-or-nothing.
func (s *Service) CreateGroup(ctx context.Context, in CreateGroupInput) (models.Conversation, error) {
	name, err := normalizeName(in.Name)
	if err != nil {
		return models.Conversation{}, err
	}
	participants := uniqueFriendIDs(in.CallerID, in.ParticipantIDs)
	if len(participants) == 0 {
		return models.Conversation{}, models.ErrMissingParticipants()
	}
	if len(participants) > MaxGroupFriends {
		return models.Conversation{}, models.ErrGroupTooLarge()
	}

	var (
		result   models.Conversation
		replayed bool
	)
	err = s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		if in.IdempotencyKey != nil {
			record, err := tx.GetIdempotencyRecord(ctx, in.CallerID, EndpointCreateGroup, in.IdempotencyKey.Key)
			switch {
			case err == nil:
				if record.RequestHash != in.IdempotencyKey.RequestHash {
					return models.ErrIdempotencyKeyReuse()
				}
				existing, err := tx.GetConversation(ctx, record.ResourceID)
				if err != nil {
					return err
				}
				result = *existing
				replayed = true
				return nil
			case errors.Is(err, repository.ErrNoRows):
			default:
				return err
			}
		}

		for _, participantID := range participants {
			if _, err := tx.GetFriendship(ctx, in.CallerID, participantID); err != nil {
				if errors.Is(err, repository.ErrNoRows) {
					return models.ErrNotFriendsValidation()
				}
				return err
			}
		}

		now := s.clock.Now()
		conversationID := uuid.New()
		members := make([]uuid.UUID, 0, len(participants)+1)
		members = append(members, in.CallerID)
		members = append(members, participants...)

		rows := make([]models.Participant, 0, len(members))
		for _, memberID := range members {
			rows = append(rows, models.Participant{ConversationID: conversationID, UserID: memberID, JoinedAt: now})
		}
		created := models.Conversation{
			ID:           conversationID,
			Kind:         models.ConversationGroup,
			Name:         name,
			CreatedBy:    in.CallerID,
			Version:      1,
			CreatedAt:    now,
			UpdatedAt:    now,
			Participants: rows,
		}
		if err := tx.InsertConversation(ctx, created); err != nil {
			return err
		}
		record, err := events.NewConversationOutbox(events.ConversationEvent{
			ConversationID: conversationID,
			Kind:           created.Kind,
			Change:         events.ChangeCreated,
			Name:           name,
			ParticipantIDs: members,
			Version:        created.Version,
			TraceID:        in.TraceID,
			OccurredAt:     now,
		})
		if err != nil {
			return err
		}
		if err := tx.InsertOutboxRecord(ctx, *record); err != nil {
			return err
		}
		if in.IdempotencyKey != nil {
			if err := tx.InsertIdempotencyRecord(ctx, models.IdempotencyRecord{
				UserID:      in.CallerID,
				Endpoint:    EndpointCreateGroup,
				Key:         in.IdempotencyKey.Key,
				RequestHash: in.IdempotencyKey.RequestHash,
				StatusCode:  http.StatusCreated,
				ResourceID:  conversationID,
				CreatedAt:   now,
			}); err != nil {
				if errors.Is(err, repository.ErrUniqueViolation) {
					return models.ErrIdempotencyKeyReuse()
				}
				return err
			}
		}
		result = created
		return nil
	})
	if err != nil {
		return models.Conversation{}, err
	}
	if !replayed {
		s.metrics.RecordCreated(models.ConversationGroup)
	}
	return result, nil
}

// AddParticipants adds the caller's friends to a group conversation. Caller
// must be a current participant; the addition is all-or-nothing.
func (s *Service) AddParticipants(ctx context.Context, callerID, conversationID uuid.UUID, userIDs []uuid.UUID, traceID string) (models.Conversation, error) {
	var (
		result     models.Conversation
		addedCount int
	)
	err := s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		conversation, err := tx.GetConversation(ctx, conversationID)
		if err != nil {
			if errors.Is(err, repository.ErrNoRows) {
				return models.ErrConversationNotFound()
			}
			return err
		}
		if !conversation.IsActiveParticipant(callerID) {
			return models.ErrNotAParticipant()
		}
		if conversation.Kind != models.ConversationGroup {
			return models.ErrGroupOnlyOperation()
		}

		candidates := uniqueFriendIDs(callerID, userIDs)
		active := make(map[uuid.UUID]struct{}, len(conversation.Participants))
		for _, participant := range conversation.ActiveParticipants() {
			active[participant.UserID] = struct{}{}
		}
		toAdd := make([]uuid.UUID, 0, len(candidates))
		for _, candidate := range candidates {
			if _, exists := active[candidate]; exists {
				continue
			}
			toAdd = append(toAdd, candidate)
		}
		if len(toAdd) == 0 {
			result = *conversation
			return nil
		}

		for _, candidate := range toAdd {
			if _, err := tx.GetFriendship(ctx, callerID, candidate); err != nil {
				if errors.Is(err, repository.ErrNoRows) {
					return models.ErrNotFriendsValidation()
				}
				return err
			}
		}
		if len(active)+len(toAdd) > MaxParticipants {
			return models.ErrGroupFull()
		}

		now := s.clock.Now()
		for _, candidate := range toAdd {
			if _, err := tx.UpsertParticipant(ctx, models.Participant{
				ConversationID: conversationID,
				UserID:         candidate,
				JoinedAt:       now,
			}); err != nil {
				return err
			}
		}
		if _, err := tx.BumpConversationVersion(ctx, conversationID, now); err != nil {
			return err
		}
		updated, err := tx.GetConversation(ctx, conversationID)
		if err != nil {
			return err
		}
		causationID, err := previousEventID(ctx, tx, models.AggregateTypeConversation, conversationID)
		if err != nil {
			return err
		}
		record, err := events.NewConversationOutbox(events.ConversationEvent{
			ConversationID: conversationID,
			Kind:           updated.Kind,
			Change:         events.ChangeParticipantsChanged,
			Name:           updated.Name,
			ParticipantIDs: updated.ActiveParticipantIDs(),
			AddedUserIDs:   toAdd,
			Version:        updated.Version,
			TraceID:        traceID,
			CorrelationID:  events.CorrelationFor(conversationID),
			CausationID:    causationID,
			OccurredAt:     now,
		})
		if err != nil {
			return err
		}
		if err := tx.InsertOutboxRecord(ctx, *record); err != nil {
			return err
		}
		result = *updated
		addedCount = len(toAdd)
		return nil
	})
	if err != nil {
		return models.Conversation{}, err
	}
	s.metrics.RecordParticipantsAdded(addedCount)
	return result, nil
}

// Leave marks the caller's participation as ended. Direct conversations cannot
// be left.
func (s *Service) Leave(ctx context.Context, callerID, conversationID uuid.UUID, traceID string) error {
	return s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		conversation, err := tx.GetConversation(ctx, conversationID)
		if err != nil {
			if errors.Is(err, repository.ErrNoRows) {
				return models.ErrConversationNotFound()
			}
			return err
		}
		if !conversation.IsActiveParticipant(callerID) {
			return models.ErrConversationNotFound()
		}
		if conversation.Kind != models.ConversationGroup {
			return models.ErrGroupOnlyOperation()
		}

		now := s.clock.Now()
		left, err := tx.MarkParticipantLeft(ctx, conversationID, callerID, now)
		if err != nil {
			return err
		}
		if !left {
			return models.ErrConversationNotFound()
		}
		if _, err := tx.BumpConversationVersion(ctx, conversationID, now); err != nil {
			return err
		}
		updated, err := tx.GetConversation(ctx, conversationID)
		if err != nil {
			return err
		}
		causationID, err := previousEventID(ctx, tx, models.AggregateTypeConversation, conversationID)
		if err != nil {
			return err
		}
		record, err := events.NewConversationOutbox(events.ConversationEvent{
			ConversationID: conversationID,
			Kind:           updated.Kind,
			Change:         events.ChangeParticipantsChanged,
			Name:           updated.Name,
			ParticipantIDs: updated.ActiveParticipantIDs(),
			RemovedUserIDs: []uuid.UUID{callerID},
			Version:        updated.Version,
			TraceID:        traceID,
			CorrelationID:  events.CorrelationFor(conversationID),
			CausationID:    causationID,
			OccurredAt:     now,
		})
		if err != nil {
			return err
		}
		return tx.InsertOutboxRecord(ctx, *record)
	})
}

// Rename updates a group conversation's name (1..100 characters).
func (s *Service) Rename(ctx context.Context, callerID, conversationID uuid.UUID, name string, traceID string) (models.Conversation, error) {
	var result models.Conversation
	err := s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		conversation, err := tx.GetConversation(ctx, conversationID)
		if err != nil {
			if errors.Is(err, repository.ErrNoRows) {
				return models.ErrConversationNotFound()
			}
			return err
		}
		if !conversation.IsActiveParticipant(callerID) {
			return models.ErrNotAParticipant()
		}
		if conversation.Kind != models.ConversationGroup {
			return models.ErrGroupOnlyOperation()
		}
		normalized, err := normalizeName(&name)
		if err != nil {
			return err
		}

		now := s.clock.Now()
		if _, err := tx.UpdateConversationName(ctx, conversationID, *normalized, now); err != nil {
			return err
		}
		updated, err := tx.GetConversation(ctx, conversationID)
		if err != nil {
			return err
		}
		causationID, err := previousEventID(ctx, tx, models.AggregateTypeConversation, conversationID)
		if err != nil {
			return err
		}
		record, err := events.NewConversationOutbox(events.ConversationEvent{
			ConversationID: conversationID,
			Kind:           updated.Kind,
			Change:         events.ChangeRenamed,
			Name:           updated.Name,
			ParticipantIDs: updated.ActiveParticipantIDs(),
			Version:        updated.Version,
			TraceID:        traceID,
			CorrelationID:  events.CorrelationFor(conversationID),
			CausationID:    causationID,
			OccurredAt:     now,
		})
		if err != nil {
			return err
		}
		if err := tx.InsertOutboxRecord(ctx, *record); err != nil {
			return err
		}
		result = *updated
		return nil
	})
	if err != nil {
		return models.Conversation{}, err
	}
	return result, nil
}

// List returns the caller's conversations with active participation, newest
// first.
func (s *Service) List(ctx context.Context, callerID uuid.UUID, cursor *models.Cursor, limit int) ([]models.Conversation, *string, error) {
	var (
		conversations []models.Conversation
		next          *string
	)
	err := s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		fetched, err := tx.ListConversations(ctx, callerID, cursor, repository.PaginationFetchSize(limit))
		if err != nil {
			return err
		}
		next = repository.NextCursor(fetched, limit, func(conversation models.Conversation) models.Cursor {
			return models.Cursor{CreatedAt: conversation.CreatedAt, ID: conversation.ID}
		})
		if len(fetched) > limit {
			fetched = fetched[:limit]
		}
		conversations = fetched
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return conversations, next, nil
}

// uniqueFriendIDs deduplicates the submitted ids and drops the caller, who is
// always a participant already.
func uniqueFriendIDs(callerID uuid.UUID, ids []uuid.UUID) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(ids))
	out := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if id == uuid.Nil || id == callerID {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// normalizeName trims and validates a group name, returning nil for an absent
// name.
func normalizeName(name *string) (*string, error) {
	if name == nil {
		return nil, nil
	}
	trimmed := strings.TrimSpace(*name)
	if trimmed == "" || utf8.RuneCountInString(trimmed) > MaxNameLength {
		return nil, models.ErrInvalidName()
	}
	return &trimmed, nil
}

// previousEventID returns the causally preceding event of an aggregate, or nil
// for the first event of a flow.
func previousEventID(ctx context.Context, tx repository.Tx, aggregateType string, aggregateID uuid.UUID) (*uuid.UUID, error) {
	eventID, err := tx.PreviousOutboxEventID(ctx, aggregateType, aggregateID)
	if err != nil {
		if errors.Is(err, repository.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return eventID, nil
}
