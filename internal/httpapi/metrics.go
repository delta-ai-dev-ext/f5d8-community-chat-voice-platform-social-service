package httpapi

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the RED metrics from architecture §7. Both label sets are
// low-cardinality: `route` is always the Gin path template, never a raw id.
type Metrics struct {
	// Requests is `http_requests_total{route,method,status_class}`.
	Requests *prometheus.CounterVec
	// Duration is `http_request_duration_seconds{route,method}`.
	Duration *prometheus.HistogramVec
}

// NewMetrics registers the HTTP metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests by route template, method, and status class.",
		}, []string{"route", "method", "status_class"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
	}
	if reg != nil {
		reg.MustRegister(metrics.Requests, metrics.Duration)
	}
	return metrics
}

// Observe records one request.
func (m *Metrics) Observe(route, method string, status int, seconds float64) {
	if m == nil {
		return
	}
	statusClass := StatusClass(status)
	if m.Requests != nil {
		m.Requests.WithLabelValues(route, method, statusClass).Inc()
	}
	if m.Duration != nil {
		m.Duration.WithLabelValues(route, method).Observe(seconds)
	}
}

// StatusClass renders the `status_class` label (e.g. "2xx") for a status code.
func StatusClass(status int) string {
	if status < 100 || status > 599 {
		return "unknown"
	}
	return strconv.Itoa(status/100) + "xx"
}
