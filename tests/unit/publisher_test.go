package unit_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"social_service/internal/events"
	"social_service/internal/models"
	"social_service/internal/repository"
	"social_service/tests/mocks"
)

type publisherFixture struct {
	store   *repository.MemoryStore
	clock   *mocks.FixedClock
	broker  *mocks.FakeBroker
	metrics *events.Metrics
	pub     *events.Publisher
}

func newPublisherFixture(t *testing.T) *publisherFixture {
	t.Helper()
	store := repository.NewMemoryStore()
	clock := mocks.NewFixedClock(baseTime)
	broker := mocks.NewFakeBroker()
	metrics := events.NewMetrics(prometheus.NewRegistry())
	publisher := events.NewPublisher(store, broker, clock, zap.NewNop(), metrics)
	t.Cleanup(store.Close)
	return &publisherFixture{store: store, clock: clock, broker: broker, metrics: metrics, pub: publisher}
}

// enqueue writes one friend-request outbox row directly, as a committed
// transaction would.
func (f *publisherFixture) enqueue(t *testing.T, status models.FriendRequestStatus) models.OutboxRecord {
	t.Helper()
	record, err := events.NewFriendRequestOutbox(events.FriendRequestEvent{
		RequestID:       uuid.New(),
		SenderUserID:    uuid.New(),
		RecipientUserID: uuid.New(),
		Status:          status,
		TraceID:         fixedTraceID,
		OccurredAt:      f.clock.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, f.store.InTx(context.Background(), func(ctx context.Context, tx repository.Tx) error {
		return tx.InsertOutboxRecord(ctx, *record)
	}))
	return *record
}

func (f *publisherFixture) unpublishedCount(t *testing.T) int64 {
	t.Helper()
	var count int64
	require.NoError(t, f.store.InTx(context.Background(), func(ctx context.Context, tx repository.Tx) error {
		total, err := tx.CountUnpublishedOutbox(ctx)
		if err != nil {
			return err
		}
		count = total
		return nil
	}))
	return count
}

func TestPublisherPublishesAndMarksRows(t *testing.T) {
	t.Parallel()

	fixture := newPublisherFixture(t)
	first := fixture.enqueue(t, models.FriendRequestPending)
	fixture.clock.Advance(time.Second)
	second := fixture.enqueue(t, models.FriendRequestAccepted)

	published, err := fixture.pub.PublishBatch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, published)
	assert.Zero(t, fixture.unpublishedCount(t), "published rows are marked in the outbox")

	messages := fixture.broker.Messages()
	require.Len(t, messages, 2)
	// Rows are published in outbox order (created_at, then id).
	assert.Equal(t, first.ID.String(), envelopeEventID(t, messages[0]))
	assert.Equal(t, second.ID.String(), envelopeEventID(t, messages[1]))

	for _, message := range messages {
		assert.Equal(t, events.Topic, message.Topic)
		assert.Equal(t, fixedTraceID, message.Headers[events.HeaderTraceID])
		assert.Equal(t, events.EventTypeFriendRequestChanged, message.Headers[events.HeaderEventType])
	}

	// A second batch finds nothing to do.
	published, err = fixture.pub.PublishBatch(context.Background())
	require.NoError(t, err)
	assert.Zero(t, published)
	assert.Len(t, fixture.broker.Messages(), 2)
}

func TestPublisherKeepsRowsOnBrokerFailure(t *testing.T) {
	t.Parallel()

	fixture := newPublisherFixture(t)
	fixture.enqueue(t, models.FriendRequestPending)
	fixture.broker.FailNext(1)

	published, err := fixture.pub.PublishBatch(context.Background())
	require.Error(t, err)
	assert.Zero(t, published)
	assert.Equal(t, int64(1), fixture.unpublishedCount(t), "an unacknowledged row stays unpublished for retry")

	published, err = fixture.pub.PublishBatch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, published)
	assert.Zero(t, fixture.unpublishedCount(t))
}

func TestPublisherDiscardsMalformedRows(t *testing.T) {
	t.Parallel()

	fixture := newPublisherFixture(t)
	broken := models.OutboxRecord{
		ID:            uuid.New(),
		AggregateType: models.AggregateTypeFriendRequest,
		AggregateID:   uuid.New(),
		EventType:     events.EventTypeFriendRequestChanged,
		EventVersion:  events.EventVersion,
		Topic:         events.Topic,
		PartitionKey:  uuid.New().String(),
		CorrelationID: uuid.New(),
		TraceID:       fixedTraceID,
		Payload:       []byte("not-json"),
		CreatedAt:     fixture.clock.Now(),
	}
	require.NoError(t, fixture.store.InTx(context.Background(), func(ctx context.Context, tx repository.Tx) error {
		return tx.InsertOutboxRecord(ctx, broken)
	}))

	published, err := fixture.pub.PublishBatch(context.Background())
	require.NoError(t, err)
	assert.Zero(t, published, "a poison row is not published")
	assert.Zero(t, fixture.unpublishedCount(t), "a poison row must not block the loop forever")
	assert.Zero(t, fixture.broker.MessageCount())
}

func TestPublisherRunDrainsAndStops(t *testing.T) {
	t.Parallel()

	fixture := newPublisherFixture(t)
	fixture.enqueue(t, models.FriendRequestPending)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		fixture.pub.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return fixture.broker.MessageCount() == 1
	}, 2*time.Second, 5*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher did not stop after cancellation")
	}
	assert.Zero(t, fixture.unpublishedCount(t))
}

func TestPublisherRetriesWhileBrokerUnhealthy(t *testing.T) {
	t.Parallel()

	fixture := newPublisherFixture(t)
	fixture.enqueue(t, models.FriendRequestPending)
	fixture.broker.FailNext(2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fixture.pub.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return fixture.broker.MessageCount() == 1
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	<-done
}

func envelopeEventID(t *testing.T, message mocks.SentMessage) string {
	t.Helper()
	decoded, err := message.DecodedMessage()
	require.NoError(t, err)
	return decoded.Envelope.EventID
}
