package events

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds the event-publishing metrics from architecture §7.
type Metrics struct {
	// Published is `social_events_published_total{event_type}`.
	Published *prometheus.CounterVec
	// PublishDuration is `kafka_publish_duration_seconds{topic}`.
	PublishDuration *prometheus.HistogramVec
	// OutboxUnpublished is the `social_outbox_unpublished` gauge.
	OutboxUnpublished prometheus.Gauge
}

// NewMetrics registers the event metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		Published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "social_events_published_total",
			Help: "Number of outbox events successfully published to Kafka.",
		}, []string{"event_type"}),
		PublishDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kafka_publish_duration_seconds",
			Help:    "Kafka publish latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"topic"}),
		OutboxUnpublished: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "social_outbox_unpublished",
			Help: "Number of event_outbox rows not yet published.",
		}),
	}
	if reg != nil {
		reg.MustRegister(metrics.Published, metrics.PublishDuration, metrics.OutboxUnpublished)
	}
	// Pre-initialise the documented label values so the series exist from
	// startup rather than appearing on first publish.
	for _, eventType := range []string{EventTypeFriendRequestChanged, EventTypeConversationChanged} {
		metrics.Published.WithLabelValues(eventType)
	}
	metrics.PublishDuration.WithLabelValues(Topic)
	return metrics
}

// RecordPublished counts one published event.
func (m *Metrics) RecordPublished(eventType string) {
	if m == nil || m.Published == nil {
		return
	}
	m.Published.WithLabelValues(eventType).Inc()
}

// ObservePublish records one publish latency observation.
func (m *Metrics) ObservePublish(topic string, seconds float64) {
	if m == nil || m.PublishDuration == nil {
		return
	}
	m.PublishDuration.WithLabelValues(topic).Observe(seconds)
}

// SetUnpublished records the current outbox depth.
func (m *Metrics) SetUnpublished(value int64) {
	if m == nil || m.OutboxUnpublished == nil {
		return
	}
	m.OutboxUnpublished.Set(float64(value))
}
