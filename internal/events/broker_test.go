package events

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"social_service/internal/config"
)

func baseKafkaConfig() config.KafkaConfig {
	return config.KafkaConfig{
		Brokers:  []string{"127.0.0.1:9092"},
		ClientID: "social_service",
	}
}

func TestNewSaramaConfig(t *testing.T) {
	t.Parallel()

	t.Run("idempotent sync producer", func(t *testing.T) {
		t.Parallel()

		saramaConfig, err := newSaramaConfig(baseKafkaConfig())
		require.NoError(t, err)
		assert.True(t, saramaConfig.Producer.Idempotent)
		assert.Equal(t, sarama.WaitForAll, saramaConfig.Producer.RequiredAcks)
		assert.Equal(t, 1, saramaConfig.Net.MaxOpenRequests, "idempotent mode requires a single in-flight request")
		assert.True(t, saramaConfig.Producer.Return.Successes)
		assert.Positive(t, saramaConfig.Producer.Retry.Max)
		assert.Equal(t, "social_service", saramaConfig.ClientID)
		assert.False(t, saramaConfig.Net.SASL.Enable)
		assert.False(t, saramaConfig.Net.TLS.Enable)
	})

	t.Run("no brokers", func(t *testing.T) {
		t.Parallel()

		_, err := newSaramaConfig(config.KafkaConfig{})
		require.Error(t, err)
	})

	t.Run("plain sasl and tls", func(t *testing.T) {
		t.Parallel()

		cfg := baseKafkaConfig()
		cfg.SASLMechanism = "plain"
		cfg.SASLUsername = "user"
		cfg.SASLPassword = "secret"
		cfg.TLSEnabled = true

		saramaConfig, err := newSaramaConfig(cfg)
		require.NoError(t, err)
		assert.True(t, saramaConfig.Net.SASL.Enable)
		assert.Equal(t, sarama.SASLTypePlaintext, string(saramaConfig.Net.SASL.Mechanism))
		assert.Equal(t, "user", saramaConfig.Net.SASL.User)
		assert.True(t, saramaConfig.Net.TLS.Enable)
		require.NotNil(t, saramaConfig.Net.TLS.Config)
		assert.Equal(t, uint16(0x0303), saramaConfig.Net.TLS.Config.MinVersion)
	})

	t.Run("scram mechanisms", func(t *testing.T) {
		t.Parallel()

		for _, test := range []struct {
			mechanism string
			expected  string
		}{
			{mechanism: "scram-sha-256", expected: sarama.SASLTypeSCRAMSHA256},
			{mechanism: "scram-sha-512", expected: sarama.SASLTypeSCRAMSHA512},
			{mechanism: "SCRAM-SHA-512", expected: sarama.SASLTypeSCRAMSHA512},
		} {
			cfg := baseKafkaConfig()
			cfg.SASLMechanism = test.mechanism
			cfg.SASLUsername = "user"
			cfg.SASLPassword = "secret"

			saramaConfig, err := newSaramaConfig(cfg)
			require.NoError(t, err, test.mechanism)
			assert.Equal(t, test.expected, string(saramaConfig.Net.SASL.Mechanism))
			require.NotNil(t, saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
		}
	})

	t.Run("unsupported mechanism", func(t *testing.T) {
		t.Parallel()

		cfg := baseKafkaConfig()
		cfg.SASLMechanism = "gssapi"
		_, err := newSaramaConfig(cfg)
		require.Error(t, err)
	})
}

func TestKafkaBrokerWithoutConnection(t *testing.T) {
	t.Parallel()

	broker, err := NewKafkaBroker(baseKafkaConfig(), zap.NewNop())
	require.NoError(t, err)
	assert.False(t, broker.Healthy(), "the producer reports unhealthy until it connects")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err = broker.Send(cancelled, Topic, "key", []byte("{}"), nil)
	assert.ErrorIs(t, err, context.Canceled)

	require.NoError(t, broker.Close(), "closing a never-connected producer is a no-op")
	require.NoError(t, broker.Close())
}

func TestKafkaBrokerConnectFailure(t *testing.T) {
	t.Parallel()

	cfg := baseKafkaConfig()
	cfg.Brokers = []string{"127.0.0.1:1"}

	broker, err := NewKafkaBroker(cfg, zap.NewNop())
	require.NoError(t, err)

	started := time.Now()
	err = broker.Connect()
	require.Error(t, err)
	assert.Less(t, time.Since(started), 60*time.Second, "connect attempts are bounded")
	assert.False(t, broker.Healthy())

	// A send on an unreachable broker fails and leaves the row for retry.
	sendErr := broker.Send(context.Background(), Topic, "key", []byte("{}"), map[string]string{HeaderTraceID: "trace"})
	require.Error(t, sendErr)
	require.NoError(t, broker.Close())
}

func TestNewKafkaBrokerRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := NewKafkaBroker(config.KafkaConfig{}, nil)
	require.Error(t, err)

	cfg := baseKafkaConfig()
	cfg.SASLMechanism = "gssapi"
	_, err = NewKafkaBroker(cfg, nil)
	require.Error(t, err)
}

func TestSCRAMClient(t *testing.T) {
	t.Parallel()

	for _, hash := range []scramHash{scramSHA256, scramSHA512} {
		client := newSCRAMClientGenerator(hash)()
		require.NotNil(t, client)
		assert.False(t, client.Done())

		// Stepping before Begin is invalid.
		_, err := client.Step("challenge")
		require.Error(t, err)

		require.NoError(t, client.Begin("user", "password", ""))
		first, err := client.Step("")
		require.NoError(t, err)
		assert.Contains(t, first, "n=user")

		// A malformed server-first message is rejected.
		_, err = client.Step("r=nonce,s=invalid-base64,i=4096")
		require.Error(t, err)
		assert.False(t, client.Done())
	}
}

func TestSCRAMHashGenerator(t *testing.T) {
	t.Parallel()

	sha256Client := newSCRAMClientGenerator(scramSHA256)().(*scramClient)
	sha512Client := newSCRAMClientGenerator(scramSHA512)().(*scramClient)

	assert.Equal(t, 32, sha256Client.hashGenerator()().Size(), "sha-256 digest size")
	assert.Equal(t, 64, sha512Client.hashGenerator()().Size(), "sha-512 digest size")

	_, err := sha256Client.Step("challenge")
	assert.ErrorIs(t, err, ErrBrokerUnavailable, "Step before Begin is rejected")
	assert.False(t, sha256Client.Done())
}
