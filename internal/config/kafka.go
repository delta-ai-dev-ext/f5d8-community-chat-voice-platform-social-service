package config

// KafkaConfig is the producer configuration handed to the events package.
type KafkaConfig struct {
	Brokers       []string
	ClientID      string
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	TLSEnabled    bool
}

// Kafka derives the producer configuration from the validated service config.
func (c *Config) Kafka() KafkaConfig {
	return KafkaConfig{
		Brokers:       c.KafkaBrokers,
		ClientID:      c.KafkaClientID,
		SASLMechanism: c.KafkaSASLMechanism,
		SASLUsername:  c.KafkaSASLUsername,
		SASLPassword:  c.KafkaSASLPassword,
		TLSEnabled:    c.KafkaTLSEnabled,
	}
}
