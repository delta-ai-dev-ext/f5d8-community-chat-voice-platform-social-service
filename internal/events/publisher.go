package events

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"social_service/internal/models"
	"social_service/internal/repository"
)

// Publisher tuning defaults.
const (
	// DefaultBatchSize is how many outbox rows one publish batch takes.
	DefaultBatchSize = 50
	// DefaultPollInterval is the idle poll cadence of the publisher loop.
	DefaultPollInterval = 250 * time.Millisecond
	// MaxBackoff bounds the exponential retry backoff after publish failures.
	MaxBackoff = 30 * time.Second
)

// Publisher drains the transactional outbox to Kafka. Rows are marked
// published only after the broker acknowledged the write, which is what makes
// delivery at-least-once and crash-safe.
type Publisher struct {
	store        repository.Store
	broker       Broker
	clock        models.Clock
	logger       *zap.Logger
	metrics      *Metrics
	batchSize    int
	pollInterval time.Duration
}

// NewPublisher builds a publisher.
func NewPublisher(store repository.Store, broker Broker, clock models.Clock, logger *zap.Logger, metrics *Metrics) *Publisher {
	if logger == nil {
		logger = zap.NewNop()
	}
	if clock == nil {
		clock = models.RealClock{}
	}
	return &Publisher{
		store:        store,
		broker:       broker,
		clock:        clock,
		logger:       logger,
		metrics:      metrics,
		batchSize:    DefaultBatchSize,
		pollInterval: DefaultPollInterval,
	}
}

// Run publishes pending outbox rows until ctx is cancelled, backing off
// exponentially (bounded) while the broker is unreachable.
func (p *Publisher) Run(ctx context.Context) {
	backoff := p.pollInterval
	for {
		if ctx.Err() != nil {
			return
		}
		published, err := p.PublishBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("outbox publish failed",
				zap.String("operation", "outbox_publish"),
				zap.String("status", "error"),
				zap.String("trace_id", models.TraceIDFromContext(ctx)),
				zap.String("detail", err.Error()),
			)
			backoff *= 2
			if backoff > MaxBackoff {
				backoff = MaxBackoff
			}
			if !sleep(ctx, backoff) {
				return
			}
			continue
		}
		backoff = p.pollInterval
		if published == 0 && !sleep(ctx, p.pollInterval) {
			return
		}
	}
}

// errDiscarded marks a poison outbox row that was marked published without
// being produced, so the loop cannot stall on it forever.
var errDiscarded = errors.New("events: outbox record discarded")

// PublishBatch publishes up to one batch of unpublished rows in outbox order.
// The returned count is how many rows were successfully published.
func (p *Publisher) PublishBatch(ctx context.Context) (int, error) {
	records, err := p.fetchBatch(ctx)
	if err != nil {
		return 0, err
	}
	published := 0
	for _, record := range records {
		if ctx.Err() != nil {
			return published, ctx.Err()
		}
		if err := p.publishOne(ctx, record); err != nil {
			p.refreshDepth(ctx)
			if errors.Is(err, errDiscarded) {
				continue
			}
			return published, err
		}
		published++
	}
	p.refreshDepth(ctx)
	return published, nil
}

func (p *Publisher) fetchBatch(ctx context.Context) ([]models.OutboxRecord, error) {
	var records []models.OutboxRecord
	err := p.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		fetched, err := tx.ListUnpublishedOutbox(ctx, p.batchSize)
		if err != nil {
			return err
		}
		records = fetched
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (p *Publisher) publishOne(ctx context.Context, record models.OutboxRecord) error {
	message, err := BuildMessage(record, p.clock.Now())
	if err != nil {
		// An unbuildable record can never succeed; mark it published so the
		// loop keeps draining instead of retrying a poison row forever.
		p.logger.Error("discarding malformed outbox record",
			zap.String("operation", "outbox_publish"),
			zap.String("status", "discarded"),
			zap.String("event_type", record.EventType),
			zap.String("detail", err.Error()),
		)
		if err := p.markPublished(ctx, record); err != nil {
			return err
		}
		return errDiscarded
	}
	wire, err := message.Marshal()
	if err != nil {
		p.logger.Error("discarding unmarshalable outbox record",
			zap.String("operation", "outbox_publish"),
			zap.String("status", "discarded"),
			zap.String("event_type", record.EventType),
			zap.String("detail", err.Error()),
		)
		if err := p.markPublished(ctx, record); err != nil {
			return err
		}
		return errDiscarded
	}

	started := p.clock.Now()
	if err := p.broker.Send(ctx, record.Topic, record.PartitionKey, wire, message.Headers()); err != nil {
		return err
	}
	p.metrics.ObservePublish(record.Topic, p.clock.Now().Sub(started).Seconds())
	if err := p.markPublished(ctx, record); err != nil {
		return err
	}
	p.metrics.RecordPublished(record.EventType)
	p.logger.Info("event published",
		zap.String("operation", "outbox_publish"),
		zap.String("status", "ok"),
		zap.String("event_type", record.EventType),
		zap.String("trace_id", message.Envelope.TraceID),
	)
	return nil
}

func (p *Publisher) markPublished(ctx context.Context, record models.OutboxRecord) error {
	return p.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		return tx.MarkOutboxPublished(ctx, []uuid.UUID{record.ID}, p.clock.Now())
	})
}

// refreshDepth updates the social_outbox_unpublished gauge.
func (p *Publisher) refreshDepth(ctx context.Context) {
	var depth int64
	err := p.store.InTx(ctx, func(ctx context.Context, tx repository.Tx) error {
		count, err := tx.CountUnpublishedOutbox(ctx)
		if err != nil {
			return err
		}
		depth = count
		return nil
	})
	if err != nil {
		return
	}
	p.metrics.SetUnpublished(depth)
}

func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
