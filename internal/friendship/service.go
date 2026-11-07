// Package friendship implements the friend-request lifecycle state machine and
// the friendship rules, committing every state change together with its outbox
// event (ADR-002).
package friendship

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"social_service/internal/events"
	"social_service/internal/models"
	"social_service/internal/repository"
)

// Idempotency endpoints (the key namespace is per endpoint).
const (
	EndpointSendFriendRequest = "POST /friend-requests"
)

// Friend-request lifecycle metric labels.
const (
	ActionSent      = "sent"
	ActionAccepted  = "accepted"
	ActionDeclined  = "declined"
	ActionCancelled = "cancelled"
)

// Metrics holds the friendship business metrics from architecture §7.
type Metrics struct {
	// Requests is `social_friend_requests_total{action}`.
	Requests *prometheus.CounterVec
	// Friendships is the `social_friendships_total` gauge.
	Friendships prometheus.Gauge
}

// NewMetrics registers the friendship metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "social_friend_requests_total",
			Help: "Friend-request lifecycle transitions.",
		}, []string{"action"}),
		Friendships: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "social_friendships_total",
			Help: "Current number of friendship pairs.",
		}),
	}
	if reg != nil {
		reg.MustRegister(metrics.Requests, metrics.Friendships)
	}
	// Pre-initialise the documented label values so the series exist from
	// startup rather than appearing on first use.
	for _, action := range []string{ActionSent, ActionAccepted, ActionDeclined, ActionCancelled} {
		metrics.Requests.WithLabelValues(action)
	}
	return metrics
}

// RecordRequest counts one lifecycle transition.
func (m *Metrics) RecordRequest(action string) {
	if m == nil || m.Requests == nil {
		return
	}
	m.Requests.WithLabelValues(action).Inc()
}

// SetFriendships records the current friendship count.
func (m *Metrics) SetFriendships(count int64) {
	if m == nil || m.Friendships == nil {
		return
	}
	m.Friendships.Set(float64(count))
}

// Service is the friendship domain service.
type Service struct {
	store   repository.Store
	clock   models.Clock
	logger  *zap.Logger
	metrics *Metrics
}

// NewService builds the friendship service.
func NewService(store repository.Store, clock models.Clock, logger *zap.Logger, metrics *Metrics) *Service {
	if clock == nil {
		clock = models.RealClock{}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{store: store, clock: clock, logger: logger, metrics: metrics}
}

// SendInput describes one send-friend-request call.
type SendInput struct {
	CallerID       uuid.UUID
	RecipientID    uuid.UUID
	TraceID        string
	IdempotencyKey *models.IdempotencyKey
}

// Send creates a pending friend request and its EVT-007 event.
func (s *Service) Send(ctx context.Context, in SendInput) (models.FriendRequest, error) {
	if in.CallerID == in.RecipientID {
		return models.FriendRequest{}, models.ErrSelfRequest()
	}

	var (
		result   models.FriendRequest
		replayed bool
	)
	err := s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		if in.IdempotencyKey != nil {
			record, err := tx.GetIdempotencyRecord(ctx, in.CallerID, EndpointSendFriendRequest, in.IdempotencyKey.Key)
			switch {
			case err == nil:
				if record.RequestHash != in.IdempotencyKey.RequestHash {
					return models.ErrIdempotencyKeyReuse()
				}
				existing, err := tx.GetFriendRequest(ctx, record.ResourceID)
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

		if _, err := tx.GetFriendship(ctx, in.CallerID, in.RecipientID); err == nil {
			return models.ErrAlreadyFriends()
		} else if !errors.Is(err, repository.ErrNoRows) {
			return err
		}

		if _, err := tx.PendingFriendRequestBetween(ctx, in.CallerID, in.RecipientID); err == nil {
			return models.ErrRequestPending()
		} else if !errors.Is(err, repository.ErrNoRows) {
			return err
		}

		now := s.clock.Now()
		request := models.FriendRequest{
			ID:              uuid.New(),
			SenderUserID:    in.CallerID,
			RecipientUserID: in.RecipientID,
			Status:          models.FriendRequestPending,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		if err := tx.InsertFriendRequest(ctx, request); err != nil {
			if errors.Is(err, repository.ErrUniqueViolation) {
				return models.ErrRequestPending()
			}
			if errors.Is(err, repository.ErrCheckViolation) {
				return models.ErrSelfRequest()
			}
			return err
		}
		record, err := events.NewFriendRequestOutbox(events.FriendRequestEvent{
			RequestID:       request.ID,
			SenderUserID:    request.SenderUserID,
			RecipientUserID: request.RecipientUserID,
			Status:          models.FriendRequestPending,
			TraceID:         in.TraceID,
			OccurredAt:      now,
		})
		if err != nil {
			return err
		}
		if err := tx.InsertOutboxRecord(ctx, *record); err != nil {
			return err
		}
		if err := s.recordIdempotency(ctx, tx, in, models.FriendRequestPending, request.ID); err != nil {
			return err
		}
		result = request
		return nil
	})
	if err != nil {
		return models.FriendRequest{}, err
	}
	if !replayed {
		s.metrics.RecordRequest(ActionSent)
	}
	return result, nil
}

func (s *Service) recordIdempotency(ctx context.Context, tx repository.Tx, in SendInput, _ models.FriendRequestStatus, resourceID uuid.UUID) error {
	if in.IdempotencyKey == nil {
		return nil
	}
	err := tx.InsertIdempotencyRecord(ctx, models.IdempotencyRecord{
		UserID:      in.CallerID,
		Endpoint:    EndpointSendFriendRequest,
		Key:         in.IdempotencyKey.Key,
		RequestHash: in.IdempotencyKey.RequestHash,
		StatusCode:  201,
		ResourceID:  resourceID,
		CreatedAt:   s.clock.Now(),
	})
	if errors.Is(err, repository.ErrUniqueViolation) {
		// A concurrent request with the same key won the race; the caller should
		// retry, which then replays the stored result.
		return models.ErrIdempotencyKeyReuse()
	}
	return err
}

// Accept accepts a pending incoming request and creates the friendship. Repeat
// accepts return the existing friendship without emitting a second event.
func (s *Service) Accept(ctx context.Context, callerID, requestID uuid.UUID, traceID string) (models.Friendship, error) {
	var (
		friendship models.Friendship
		count      int64
		changed    bool
	)
	err := s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		request, err := tx.LockFriendRequest(ctx, requestID)
		if err != nil {
			if errors.Is(err, repository.ErrNoRows) {
				return models.ErrFriendRequestNotFound()
			}
			return err
		}
		if request.RecipientUserID != callerID {
			return models.ErrFriendRequestNotFound()
		}
		if request.Status == models.FriendRequestAccepted {
			existing, err := tx.GetFriendship(ctx, request.SenderUserID, request.RecipientUserID)
			if err != nil {
				if errors.Is(err, repository.ErrNoRows) {
					return models.ErrRequestNotPending()
				}
				return err
			}
			friendship = *existing
			return nil
		}
		if request.Status != models.FriendRequestPending {
			return models.ErrRequestNotPending()
		}

		now := s.clock.Now()
		if err := tx.UpdateFriendRequestStatus(ctx, request.ID, models.FriendRequestAccepted, now); err != nil {
			return err
		}
		lesser, greater := models.CanonicalPair(request.SenderUserID, request.RecipientUserID)
		created := models.Friendship{UserIDA: lesser, UserIDB: greater, CreatedAt: now}
		if err := tx.InsertFriendship(ctx, created); err != nil && !errors.Is(err, repository.ErrUniqueViolation) {
			return err
		}
		friendship = created

		causationID, err := previousEventID(ctx, tx, models.AggregateTypeFriendRequest, request.ID)
		if err != nil {
			return err
		}
		record, err := events.NewFriendRequestOutbox(events.FriendRequestEvent{
			RequestID:       request.ID,
			SenderUserID:    request.SenderUserID,
			RecipientUserID: request.RecipientUserID,
			Status:          models.FriendRequestAccepted,
			TraceID:         traceID,
			CorrelationID:   events.CorrelationFor(request.ID),
			CausationID:     causationID,
			OccurredAt:      now,
		})
		if err != nil {
			return err
		}
		if err := tx.InsertOutboxRecord(ctx, *record); err != nil {
			return err
		}
		total, err := tx.CountFriendships(ctx)
		if err != nil {
			return err
		}
		count = total
		changed = true
		return nil
	})
	if err != nil {
		return models.Friendship{}, err
	}
	if changed {
		s.metrics.RecordRequest(ActionAccepted)
		s.metrics.SetFriendships(count)
	}
	return friendship, nil
}

// Decline declines a pending incoming request.
func (s *Service) Decline(ctx context.Context, callerID, requestID uuid.UUID, traceID string) error {
	return s.resolve(ctx, callerID, requestID, traceID, models.FriendRequestDeclined, ActionDeclined, func(request *models.FriendRequest, caller uuid.UUID) bool {
		return request.RecipientUserID == caller
	})
}

// Cancel cancels a pending outgoing request (sender only).
func (s *Service) Cancel(ctx context.Context, callerID, requestID uuid.UUID, traceID string) error {
	return s.resolve(ctx, callerID, requestID, traceID, models.FriendRequestCancelled, ActionCancelled, func(request *models.FriendRequest, caller uuid.UUID) bool {
		return request.SenderUserID == caller
	})
}

func (s *Service) resolve(
	ctx context.Context,
	callerID, requestID uuid.UUID,
	traceID string,
	target models.FriendRequestStatus,
	action string,
	isAllowed func(request *models.FriendRequest, caller uuid.UUID) bool,
) error {
	return s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		request, err := tx.LockFriendRequest(ctx, requestID)
		if err != nil {
			if errors.Is(err, repository.ErrNoRows) {
				return models.ErrFriendRequestNotFound()
			}
			return err
		}
		if !isAllowed(request, callerID) {
			return models.ErrFriendRequestNotFound()
		}
		if request.Status != models.FriendRequestPending {
			return models.ErrRequestNotPending()
		}
		now := s.clock.Now()
		if err := tx.UpdateFriendRequestStatus(ctx, request.ID, target, now); err != nil {
			return err
		}
		causationID, err := previousEventID(ctx, tx, models.AggregateTypeFriendRequest, request.ID)
		if err != nil {
			return err
		}
		record, err := events.NewFriendRequestOutbox(events.FriendRequestEvent{
			RequestID:       request.ID,
			SenderUserID:    request.SenderUserID,
			RecipientUserID: request.RecipientUserID,
			Status:          target,
			TraceID:         traceID,
			CorrelationID:   events.CorrelationFor(request.ID),
			CausationID:     causationID,
			OccurredAt:      now,
		})
		if err != nil {
			return err
		}
		if err := tx.InsertOutboxRecord(ctx, *record); err != nil {
			return err
		}
		s.metrics.RecordRequest(action)
		return nil
	})
}

// RemoveFriend deletes the canonical friendship row for both sides. No event is
// emitted: shared group conversations are unaffected.
func (s *Service) RemoveFriend(ctx context.Context, callerID, friendID uuid.UUID) error {
	if callerID == friendID {
		return models.ErrNotFriendsNotFound()
	}
	var count int64
	err := s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		deleted, err := tx.DeleteFriendship(ctx, callerID, friendID)
		if err != nil {
			return err
		}
		if !deleted {
			return models.ErrNotFriendsNotFound()
		}
		total, err := tx.CountFriendships(ctx)
		if err != nil {
			return err
		}
		count = total
		return nil
	})
	if err != nil {
		return err
	}
	s.metrics.SetFriendships(count)
	return nil
}

// ListRequests returns the caller's pending requests for one direction.
func (s *Service) ListRequests(ctx context.Context, callerID uuid.UUID, direction models.RequestDirection, cursor *models.Cursor, limit int) ([]models.FriendRequest, *string, error) {
	var (
		requests []models.FriendRequest
		next     *string
	)
	err := s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		fetched, err := tx.ListFriendRequests(ctx, callerID, direction, cursor, repository.PaginationFetchSize(limit))
		if err != nil {
			return err
		}
		next = repository.NextCursor(fetched, limit, func(request models.FriendRequest) models.Cursor {
			return models.Cursor{CreatedAt: request.CreatedAt, ID: request.ID}
		})
		if len(fetched) > limit {
			fetched = fetched[:limit]
		}
		requests = fetched
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return requests, next, nil
}

// ListFriends returns the caller's friends, newest friendship first.
func (s *Service) ListFriends(ctx context.Context, callerID uuid.UUID, cursor *models.Cursor, limit int) ([]models.Friendship, *string, error) {
	var (
		friendships []models.Friendship
		next        *string
	)
	err := s.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		fetched, err := tx.ListFriendships(ctx, callerID, cursor, repository.PaginationFetchSize(limit))
		if err != nil {
			return err
		}
		next = repository.NextCursor(fetched, limit, func(friendship models.Friendship) models.Cursor {
			other, ok := friendship.Other(callerID)
			if !ok {
				other = friendship.UserIDA
			}
			return models.Cursor{CreatedAt: friendship.CreatedAt, ID: other}
		})
		if len(fetched) > limit {
			fetched = fetched[:limit]
		}
		friendships = fetched
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return friendships, next, nil
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
