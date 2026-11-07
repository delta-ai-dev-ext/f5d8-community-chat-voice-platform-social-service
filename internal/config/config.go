// Package config loads and validates the service's 12-factor configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment variable names.
const (
	EnvDatabaseURL               = "DATABASE_URL"
	EnvKafkaBrokers              = "KAFKA_BROKERS"
	EnvKafkaClientID             = "KAFKA_CLIENT_ID"
	EnvKafkaSASLMechanism        = "KAFKA_SASL_MECHANISM"
	EnvKafkaSASLUsername         = "KAFKA_SASL_USERNAME"
	EnvKafkaSASLPassword         = "KAFKA_SASL_PASSWORD"
	EnvKafkaTLSEnabled           = "KAFKA_TLS_ENABLED"
	EnvTokenVerificationKeys     = "TOKEN_VERIFICATION_KEYS"
	EnvTokenVerificationKeysPath = "TOKEN_VERIFICATION_KEYS_PATH"
	EnvTokenIssuer               = "TOKEN_ISSUER"
	EnvTokenAudience             = "TOKEN_AUDIENCE"
	EnvHTTPPort                  = "HTTP_PORT"
	EnvShutdownTimeout           = "SHUTDOWN_TIMEOUT_SECONDS"
	EnvLogLevel                  = "LOG_LEVEL"
)

// Defaults from the engineering guide.
const (
	DefaultKafkaClientID       = "social_service"
	DefaultHTTPPort            = 8080
	DefaultShutdownTimeoutSecs = 30
	DefaultLogLevel            = "info"
	MaxShutdownTimeoutSecs     = 300
)

// Config is the validated runtime configuration.
type Config struct {
	DatabaseURL string

	KafkaBrokers       []string
	KafkaClientID      string
	KafkaSASLMechanism string
	KafkaSASLUsername  string
	KafkaSASLPassword  string
	KafkaTLSEnabled    bool

	TokenVerificationKeysPath string
	TokenVerificationKeys     []VerificationKey
	TokenIssuer               string
	TokenAudience             string

	HTTPPort        int
	ShutdownTimeout time.Duration
	LogLevel        string
}

// Getenv is the environment accessor used by Load; tests substitute a map.
type Getenv func(string) string

// OSEnv reads from the process environment.
func OSEnv(key string) string { return os.Getenv(key) }

// Load reads and validates the configuration from the environment.
func Load() (*Config, error) {
	return LoadFrom(OSEnv)
}

// LoadFrom reads and validates the configuration using the supplied accessor.
func LoadFrom(getenv Getenv) (*Config, error) {
	if getenv == nil {
		getenv = OSEnv
	}
	cfg := &Config{
		DatabaseURL:               strings.TrimSpace(getenv(EnvDatabaseURL)),
		KafkaClientID:             valueOrDefault(getenv(EnvKafkaClientID), DefaultKafkaClientID),
		KafkaSASLMechanism:        strings.ToLower(strings.TrimSpace(getenv(EnvKafkaSASLMechanism))),
		KafkaSASLUsername:         getenv(EnvKafkaSASLUsername),
		KafkaSASLPassword:         getenv(EnvKafkaSASLPassword),
		TokenVerificationKeysPath: strings.TrimSpace(getenv(EnvTokenVerificationKeysPath)),
		TokenIssuer:               strings.TrimSpace(getenv(EnvTokenIssuer)),
		TokenAudience:             strings.TrimSpace(getenv(EnvTokenAudience)),
		LogLevel:                  strings.ToLower(valueOrDefault(getenv(EnvLogLevel), DefaultLogLevel)),
	}

	for _, broker := range strings.Split(getenv(EnvKafkaBrokers), ",") {
		if trimmed := strings.TrimSpace(broker); trimmed != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, trimmed)
		}
	}

	var errs []error
	if cfg.DatabaseURL == "" {
		errs = append(errs, required(EnvDatabaseURL))
	}
	if len(cfg.KafkaBrokers) == 0 {
		errs = append(errs, required(EnvKafkaBrokers))
	}
	if cfg.TokenIssuer == "" {
		errs = append(errs, required(EnvTokenIssuer))
	}
	if cfg.TokenAudience == "" {
		errs = append(errs, required(EnvTokenAudience))
	}
	if !validLogLevel(cfg.LogLevel) {
		errs = append(errs, fmt.Errorf("%s must be one of debug|info|warn|error, got %q", EnvLogLevel, cfg.LogLevel))
	}

	port, err := parseInteger(getenv(EnvHTTPPort), EnvHTTPPort, DefaultHTTPPort)
	if err != nil {
		errs = append(errs, err)
	} else if port < 1 || port > 65535 {
		errs = append(errs, fmt.Errorf("%s must be between 1 and 65535, got %d", EnvHTTPPort, port))
	}
	cfg.HTTPPort = port

	shutdown, err := parseInteger(getenv(EnvShutdownTimeout), EnvShutdownTimeout, DefaultShutdownTimeoutSecs)
	if err != nil {
		errs = append(errs, err)
	} else if shutdown < 1 || shutdown > MaxShutdownTimeoutSecs {
		errs = append(errs, fmt.Errorf("%s must be between 1 and %d, got %d", EnvShutdownTimeout, MaxShutdownTimeoutSecs, shutdown))
	}
	cfg.ShutdownTimeout = time.Duration(shutdown) * time.Second

	tlsEnabled, err := parseBool(getenv(EnvKafkaTLSEnabled), EnvKafkaTLSEnabled)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.KafkaTLSEnabled = tlsEnabled

	if err := validateSASL(cfg); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	keys, err := LoadVerificationKeys(getenv(EnvTokenVerificationKeys), cfg.TokenVerificationKeysPath)
	if err != nil {
		return nil, err
	}
	cfg.TokenVerificationKeys = keys
	return cfg, nil
}

func validateSASL(cfg *Config) error {
	switch cfg.KafkaSASLMechanism {
	case "":
		if cfg.KafkaSASLUsername != "" || cfg.KafkaSASLPassword != "" {
			return fmt.Errorf("%s is empty but %s/%s were provided", EnvKafkaSASLMechanism, EnvKafkaSASLUsername, EnvKafkaSASLPassword)
		}
		return nil
	case "plain", "scram-sha-256", "scram-sha-512":
	default:
		return fmt.Errorf("%s must be one of plain|scram-sha-256|scram-sha-512, got %q", EnvKafkaSASLMechanism, cfg.KafkaSASLMechanism)
	}
	if cfg.KafkaSASLUsername == "" || cfg.KafkaSASLPassword == "" {
		return fmt.Errorf("%s requires both %s and %s", EnvKafkaSASLMechanism, EnvKafkaSASLUsername, EnvKafkaSASLPassword)
	}
	return nil
}

func required(name string) error {
	return fmt.Errorf("%s is required but was not set", name)
}

func valueOrDefault(value, fallback string) string {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}
	return fallback
}

func validLogLevel(level string) bool {
	switch level {
	case "debug", "info", "warn", "error":
		return true
	default:
		return false
	}
}

func parseInteger(raw, name string, fallback int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q", name, raw)
	}
	return parsed, nil
}

func parseBool(raw, name string) (bool, error) {
	if strings.TrimSpace(raw) == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean, got %q", name, raw)
	}
	return parsed, nil
}
