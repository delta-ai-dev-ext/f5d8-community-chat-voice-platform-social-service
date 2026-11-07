package events

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"social_service/internal/config"
)

// Broker is the narrow producer abstraction the publisher depends on. It is
// implemented by KafkaBroker in production and by an in-process fake in tests.
type Broker interface {
	// Send produces one message synchronously, mirroring the envelope fields
	// into headers. It returns only after the broker acknowledged the write.
	Send(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
	// Healthy reports whether the producer can currently reach a broker.
	Healthy() bool
	// Close flushes pending messages and releases the connection.
	Close() error
}

// ErrBrokerUnavailable is returned when the producer has no live connection.
var ErrBrokerUnavailable = errors.New("events: kafka producer is not connected")

// KafkaBroker is the sarama-based sync producer with idempotent mode enabled.
type KafkaBroker struct {
	config   config.KafkaConfig
	sarama   *sarama.Config
	logger   *zap.Logger
	mu       sync.Mutex
	producer sarama.SyncProducer
}

// NewKafkaBroker builds the producer configuration. It validates the SASL and
// TLS options but does not dial the brokers yet.
func NewKafkaBroker(cfg config.KafkaConfig, logger *zap.Logger) (*KafkaBroker, error) {
	saramaConfig, err := newSaramaConfig(cfg)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &KafkaBroker{config: cfg, sarama: saramaConfig, logger: logger}, nil
}

func newSaramaConfig(cfg config.KafkaConfig) (*sarama.Config, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = cfg.ClientID
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Retry.Backoff = 250 * time.Millisecond
	// Idempotent producing requires a single in-flight request per connection.
	saramaConfig.Net.MaxOpenRequests = 1
	saramaConfig.Metadata.Retry.Max = 5
	saramaConfig.Metadata.Full = true

	switch strings.ToLower(strings.TrimSpace(cfg.SASLMechanism)) {
	case "":
	case "plain":
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		saramaConfig.Net.SASL.User = cfg.SASLUsername
		saramaConfig.Net.SASL.Password = cfg.SASLPassword
	case "scram-sha-256":
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		saramaConfig.Net.SASL.User = cfg.SASLUsername
		saramaConfig.Net.SASL.Password = cfg.SASLPassword
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = newSCRAMClientGenerator(scramSHA256)
	case "scram-sha-512":
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		saramaConfig.Net.SASL.User = cfg.SASLUsername
		saramaConfig.Net.SASL.Password = cfg.SASLPassword
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = newSCRAMClientGenerator(scramSHA512)
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", cfg.SASLMechanism)
	}
	if cfg.TLSEnabled {
		//nolint:gosec // the platform terminates with a trusted cluster certificate
		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if err := saramaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: invalid producer configuration: %w", err)
	}
	return saramaConfig, nil
}

// Connect dials the brokers. It is safe to call again after a failure.
func (b *KafkaBroker) Connect() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.producer != nil {
		return nil
	}
	producer, err := sarama.NewSyncProducer(b.config.Brokers, b.sarama)
	if err != nil {
		return fmt.Errorf("kafka: connect: %w", err)
	}
	b.producer = producer
	b.logger.Info("kafka producer connected",
		zap.String("operation", "kafka_connect"),
		zap.String("status", "ok"),
		zap.Int("broker_count", len(b.config.Brokers)),
	)
	return nil
}

func (b *KafkaBroker) ensureProducer() (sarama.SyncProducer, error) {
	b.mu.Lock()
	producer := b.producer
	b.mu.Unlock()
	if producer != nil {
		return producer, nil
	}
	if err := b.Connect(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.producer == nil {
		return nil, ErrBrokerUnavailable
	}
	return b.producer, nil
}

// Send produces one message synchronously.
func (b *KafkaBroker) Send(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	producer, err := b.ensureProducer()
	if err != nil {
		return err
	}
	recordHeaders := make([]sarama.RecordHeader, 0, len(headers))
	for name, header := range headers {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{
			Key:   []byte(name),
			Value: []byte(header),
		})
	}
	message := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.StringEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: recordHeaders,
	}
	if _, _, err := producer.SendMessage(message); err != nil {
		return fmt.Errorf("kafka: send to %s: %w", topic, err)
	}
	return nil
}

// Healthy reports whether a producer connection is currently established.
func (b *KafkaBroker) Healthy() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.producer != nil
}

// Close flushes and closes the producer.
func (b *KafkaBroker) Close() error {
	b.mu.Lock()
	producer := b.producer
	b.producer = nil
	b.mu.Unlock()
	if producer == nil {
		return nil
	}
	if err := producer.Close(); err != nil {
		return fmt.Errorf("kafka: close: %w", err)
	}
	return nil
}
