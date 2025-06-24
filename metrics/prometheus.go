package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the application
type Metrics struct {
	// Database metrics
	RecordsWritten    prometheus.Counter
	WriteErrors       prometheus.Counter
	WriteDuration     prometheus.Histogram
	DatabaseConnections prometheus.Gauge

	// Kafka metrics
	MessagesConsumed  prometheus.Counter
	ConsumerErrors    prometheus.Counter
	BatchSize         prometheus.Histogram
	ProcessingDuration prometheus.Histogram

	// Application metrics
	ActiveWorkers     prometheus.Gauge
	TotalThroughput   prometheus.Counter
}

// NewMetrics creates and registers all Prometheus metrics
func NewMetrics() *Metrics {
	return &Metrics{
		// Database metrics
		RecordsWritten: promauto.NewCounter(prometheus.CounterOpts{
			Name: "database_records_written_total",
			Help: "Total number of records written to database",
		}),
		WriteErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "database_write_errors_total",
			Help: "Total number of database write errors",
		}),
		WriteDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "database_write_duration_seconds",
			Help:    "Database write operation duration in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		DatabaseConnections: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "database_connections_active",
			Help: "Number of active database connections",
		}),

		// Kafka metrics
		MessagesConsumed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "kafka_messages_consumed_total",
			Help: "Total number of Kafka messages consumed",
		}),
		ConsumerErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "kafka_consumer_errors_total",
			Help: "Total number of Kafka consumer errors",
		}),
		BatchSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "kafka_batch_size",
			Help:    "Size of processed message batches",
			Buckets: []float64{10, 50, 100, 500, 1000, 2500, 5000, 10000},
		}),
		ProcessingDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "kafka_processing_duration_seconds",
			Help:    "Message batch processing duration in seconds",
			Buckets: prometheus.DefBuckets,
		}),

		// Application metrics
		ActiveWorkers: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "application_active_workers",
			Help: "Number of active consumer workers",
		}),
		TotalThroughput: promauto.NewCounter(prometheus.CounterOpts{
			Name: "application_throughput_total",
			Help: "Total application throughput (records per second)",
		}),
	}
}

// RecordDatabaseWrite records a successful database write operation
func (m *Metrics) RecordDatabaseWrite(recordCount int, duration float64) {
	m.RecordsWritten.Add(float64(recordCount))
	m.WriteDuration.Observe(duration)
}

// RecordDatabaseError records a database error
func (m *Metrics) RecordDatabaseError() {
	m.WriteErrors.Inc()
}

// RecordKafkaConsumption records Kafka message consumption
func (m *Metrics) RecordKafkaConsumption(messageCount int, duration float64) {
	m.MessagesConsumed.Add(float64(messageCount))
	m.BatchSize.Observe(float64(messageCount))
	m.ProcessingDuration.Observe(duration)
}

// RecordKafkaError records a Kafka consumer error
func (m *Metrics) RecordKafkaError() {
	m.ConsumerErrors.Inc()
}

// SetActiveWorkers sets the number of active workers
func (m *Metrics) SetActiveWorkers(count int) {
	m.ActiveWorkers.Set(float64(count))
}

// SetDatabaseConnections sets the number of active database connections
func (m *Metrics) SetDatabaseConnections(count int) {
	m.DatabaseConnections.Set(float64(count))
}

// RecordThroughput records overall application throughput
func (m *Metrics) RecordThroughput(recordsPerSecond float64) {
	m.TotalThroughput.Add(recordsPerSecond)
}