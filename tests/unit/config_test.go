package unit_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"social_service/internal/config"
)

// generateKeyPair returns an RSA private key and its PKIX PEM public encoding.
func generateKeyPair(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	encoded, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)
	return privateKey, string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}))
}

func generateEd25519Keys(t *testing.T) (ed25519.PrivateKey, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	encoded, err := x509.MarshalPKIXPublicKey(publicKey)
	require.NoError(t, err)
	return privateKey, string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}))
}

func baseEnv(t *testing.T) map[string]string {
	t.Helper()
	_, pemValue := generateKeyPair(t)
	return map[string]string{
		config.EnvDatabaseURL:           "postgres://social:social@localhost:5432/social?sslmode=disable",
		config.EnvKafkaBrokers:          "broker-1:9092, broker-2:9092",
		config.EnvTokenVerificationKeys: pemValue,
		config.EnvTokenIssuer:           "identity_service",
		config.EnvTokenAudience:         "social_service",
	}
}

func loadFrom(env map[string]string) (*config.Config, error) {
	return config.LoadFrom(func(key string) string { return env[key] })
}

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := loadFrom(baseEnv(t))
	require.NoError(t, err)

	assert.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, config.DefaultKafkaClientID, cfg.KafkaClientID)
	assert.Equal(t, config.DefaultHTTPPort, cfg.HTTPPort)
	assert.Equal(t, config.DefaultLogLevel, cfg.LogLevel)
	assert.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
	assert.False(t, cfg.KafkaTLSEnabled)
	require.Len(t, cfg.TokenVerificationKeys, 1)
	assert.Equal(t, "current", cfg.TokenVerificationKeys[0].KeyID)

	assert.Equal(t, cfg.KafkaBrokers, cfg.Kafka().Brokers)
	assert.Equal(t, cfg.KafkaClientID, cfg.Kafka().ClientID)
}

func TestLoadConfigMissingRequiredVariables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		unset   string
		wantErr string
	}{
		{name: "database url", unset: config.EnvDatabaseURL, wantErr: config.EnvDatabaseURL},
		{name: "kafka brokers", unset: config.EnvKafkaBrokers, wantErr: config.EnvKafkaBrokers},
		{name: "token issuer", unset: config.EnvTokenIssuer, wantErr: config.EnvTokenIssuer},
		{name: "token audience", unset: config.EnvTokenAudience, wantErr: config.EnvTokenAudience},
		{name: "verification keys", unset: config.EnvTokenVerificationKeys, wantErr: config.EnvTokenVerificationKeys},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			env := baseEnv(t)
			delete(env, test.unset)
			_, err := loadFrom(env)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
			assert.NotContains(t, err.Error(), "BEGIN", "the error must never echo key material")
			assert.NotContains(t, err.Error(), "postgres://", "the error must never echo a secret value")
		})
	}
}

func TestLoadConfigInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{name: "port not a number", key: config.EnvHTTPPort, value: "eighty", wantErr: config.EnvHTTPPort},
		{name: "port out of range", key: config.EnvHTTPPort, value: "70000", wantErr: config.EnvHTTPPort},
		{name: "shutdown not a number", key: config.EnvShutdownTimeout, value: "soon", wantErr: config.EnvShutdownTimeout},
		{name: "shutdown zero", key: config.EnvShutdownTimeout, value: "0", wantErr: config.EnvShutdownTimeout},
		{name: "shutdown too large", key: config.EnvShutdownTimeout, value: "100000", wantErr: config.EnvShutdownTimeout},
		{name: "log level", key: config.EnvLogLevel, value: "verbose", wantErr: config.EnvLogLevel},
		{name: "tls flag", key: config.EnvKafkaTLSEnabled, value: "sometimes", wantErr: config.EnvKafkaTLSEnabled},
		{name: "sasl mechanism", key: config.EnvKafkaSASLMechanism, value: "gssapi", wantErr: config.EnvKafkaSASLMechanism},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			env := baseEnv(t)
			env[test.key] = test.value
			_, err := loadFrom(env)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

func TestLoadConfigSASLRules(t *testing.T) {
	t.Parallel()

	t.Run("mechanism requires credentials", func(t *testing.T) {
		t.Parallel()

		env := baseEnv(t)
		env[config.EnvKafkaSASLMechanism] = "scram-sha-512"
		_, err := loadFrom(env)
		require.Error(t, err)
		assert.Contains(t, err.Error(), config.EnvKafkaSASLUsername)
	})

	t.Run("credentials without mechanism", func(t *testing.T) {
		t.Parallel()

		env := baseEnv(t)
		env[config.EnvKafkaSASLUsername] = "user"
		_, err := loadFrom(env)
		require.Error(t, err)
		assert.Contains(t, err.Error(), config.EnvKafkaSASLMechanism)
	})

	t.Run("complete sasl and tls", func(t *testing.T) {
		t.Parallel()

		env := baseEnv(t)
		env[config.EnvKafkaSASLMechanism] = "PLAIN"
		env[config.EnvKafkaSASLUsername] = "user"
		env[config.EnvKafkaSASLPassword] = "secret"
		env[config.EnvKafkaTLSEnabled] = "true"
		env[config.EnvHTTPPort] = "9090"
		env[config.EnvShutdownTimeout] = "45"
		env[config.EnvLogLevel] = "debug"

		cfg, err := loadFrom(env)
		require.NoError(t, err)
		assert.Equal(t, "plain", cfg.KafkaSASLMechanism)
		assert.True(t, cfg.KafkaTLSEnabled)
		assert.Equal(t, 9090, cfg.HTTPPort)
		assert.Equal(t, 45*time.Second, cfg.ShutdownTimeout)
		assert.Equal(t, "debug", cfg.LogLevel)
	})
}

func TestLoadVerificationKeysFromPath(t *testing.T) {
	t.Parallel()

	t.Run("directory with current and next", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		_, current := generateKeyPair(t)
		_, next := generateEd25519Keys(t)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "current.pem"), []byte(current), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "next.pem"), []byte(next), 0o600))

		keys, err := config.LoadVerificationKeys("", dir)
		require.NoError(t, err)
		require.Len(t, keys, 2)
		assert.Equal(t, "current", keys[0].KeyID)
		assert.Equal(t, "next", keys[1].KeyID)
	})

	t.Run("directory with only current", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		_, current := generateKeyPair(t)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "current.pem"), []byte(current), 0o600))

		keys, err := config.LoadVerificationKeys("", dir)
		require.NoError(t, err)
		require.Len(t, keys, 1)
	})

	t.Run("single pem file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		_, current := generateKeyPair(t)
		path := filepath.Join(dir, "keys.pem")
		require.NoError(t, os.WriteFile(path, []byte(current), 0o600))

		keys, err := config.LoadVerificationKeys("", path)
		require.NoError(t, err)
		require.Len(t, keys, 1)
	})

	t.Run("json document", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		_, current := generateKeyPair(t)
		_, next := generateKeyPair(t)
		document := `{"current_key":` + quoteJSON(t, current) + `,"next_key":` + quoteJSON(t, next) + `}`
		path := filepath.Join(dir, "keys.json")
		require.NoError(t, os.WriteFile(path, []byte(document), 0o600))

		keys, err := config.LoadVerificationKeys("", path)
		require.NoError(t, err)
		require.Len(t, keys, 2)
	})

	t.Run("missing path", func(t *testing.T) {
		t.Parallel()

		_, err := config.LoadVerificationKeys("", filepath.Join(t.TempDir(), "absent"))
		require.Error(t, err)
	})

	t.Run("empty directory", func(t *testing.T) {
		t.Parallel()

		_, err := config.LoadVerificationKeys("", t.TempDir())
		require.ErrorIs(t, err, config.ErrNoVerificationKeys)
	})

	t.Run("invalid pem", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "current.pem")
		require.NoError(t, os.WriteFile(path, []byte("not a key"), 0o600))

		_, err := config.LoadVerificationKeys("", path)
		require.Error(t, err)
	})

	t.Run("missing inline source", func(t *testing.T) {
		t.Parallel()

		_, err := config.LoadVerificationKeys("", "")
		require.ErrorIs(t, err, config.ErrNoVerificationKeys)
	})
}

func TestLoadConfigFromKeysPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_, current := generateKeyPair(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "current.pem"), []byte(current), 0o600))

	env := baseEnv(t)
	delete(env, config.EnvTokenVerificationKeys)
	env[config.EnvTokenVerificationKeysPath] = dir

	cfg, err := loadFrom(env)
	require.NoError(t, err)
	require.Len(t, cfg.TokenVerificationKeys, 1)
}

func quoteJSON(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return string(encoded)
}
