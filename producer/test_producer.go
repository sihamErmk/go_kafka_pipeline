package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// TestProducer generates test data for the Kafka pipeline
type TestProducer struct {
	producer sarama.SyncProducer
	topic    string
	logger   *zap.Logger
}

// TestDataConfig holds configuration for test data generation
type TestDataConfig struct {
	Brokers       []string
	Topic         string
	RecordCount   int
	Concurrency   int
	BatchSize     int
}

// NewTestProducer creates a new test data producer
func NewTestProducer(brokers []string, topic string, logger *zap.Logger) (*TestProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Compression = sarama.CompressionLZ4
	config.Producer.Flush.Frequency = 20 * time.Millisecond
	config.Producer.Batch.Size = 65536

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &TestProducer{
		producer: producer,
		topic:    topic,
		logger:   logger,
	}, nil
}

// GenerateTestDataConcurrently generates test data with high concurrency
func (tp *TestProducer) GenerateTestDataConcurrently(ctx context.Context, config TestDataConfig) error {
	startTime := time.Now()
	
	tp.logger.Info("Starting concurrent test data generation",
		zap.Int("record_count", config.RecordCount),
		zap.Int("concurrency", config.Concurrency),
		zap.Int("batch_size", config.BatchSize),
	)

	// Calculate records per worker
	recordsPerWorker := config.RecordCount / config.Concurrency
	remainder := config.RecordCount % config.Concurrency

	var wg sync.WaitGroup
	errorChan := make(chan error, config.Concurrency)
	successCount := make(chan int, config.Concurrency)

	// Start concurrent workers
	for i := 0; i < config.Concurrency; i++ {
		wg.Add(1)
		
		workerRecords := recordsPerWorker
		if i < remainder {
			workerRecords++ // Distribute remainder across first workers
		}

		go func(workerID, recordCount int) {
			defer wg.Done()
			
			count, err := tp.generateWorkerData(ctx, workerID, recordCount, config.BatchSize)
			if err != nil {
				errorChan <- fmt.Errorf("worker %d failed: %w", workerID, err)
				return
			}
			
			successCount <- count
		}(i, workerRecords)
	}

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(errorChan)
		close(successCount)
	}()

	// Collect results
	totalSuccess := 0
	var errors []error

	for {
		select {
		case err, ok := <-errorChan:
			if !ok {
				errorChan = nil
			} else if err != nil {
				errors = append(errors, err)
			}
		case count, ok := <-successCount:
			if !ok {
				successCount = nil
			} else {
				totalSuccess += count
			}
		}

		if errorChan == nil && successCount == nil {
			break
		}
	}

	duration := time.Since(startTime)
	throughput := float64(totalSuccess) / duration.Seconds()

	if len(errors) > 0 {
		tp.logger.Error("Test data generation completed with errors",
			zap.Int("successful_records", totalSuccess),
			zap.Int("error_count", len(errors)),
			zap.Duration("duration", duration),
		)
		return fmt.Errorf("generation failed with %d errors", len(errors))
	}

	tp.logger.Info("✅ Test data generation completed successfully",
		zap.Int("total_records", totalSuccess),
		zap.Duration("duration", duration),
		zap.Float64("throughput_per_sec", throughput),
	)

	return nil
}

// generateWorkerData generates test data for a single worker
func (tp *TestProducer) generateWorkerData(ctx context.Context, workerID, recordCount, batchSize int) (int, error) {
	tp.logger.Info("Worker starting data generation",
		zap.Int("worker_id", workerID),
		zap.Int("record_count", recordCount),
	)

	successCount := 0
	batch := make([]*sarama.ProducerMessage, 0, batchSize)

	for i := 0; i < recordCount; i++ {
		select {
		case <-ctx.Done():
			return successCount, ctx.Err()
		default:
		}

		// Generate test record
		record := tp.generateTestRecord(workerID, i)
		
		// Convert to Avro-compatible JSON
		jsonData, err := json.Marshal(record)
		if err != nil {
			tp.logger.Error("Failed to marshal test record", zap.Error(err))
			continue
		}

		// Create Kafka message
		message := &sarama.ProducerMessage{
			Topic: tp.topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("worker-%d-record-%d", workerID, i)),
			Value: sarama.ByteEncoder(jsonData), // In real implementation, this would be Avro binary
		}

		batch = append(batch, message)

		// Send batch when it reaches the configured size
		if len(batch) >= batchSize {
			if err := tp.sendBatch(batch); err != nil {
				return successCount, fmt.Errorf("failed to send batch: %w", err)
			}
			successCount += len(batch)
			batch = batch[:0] // Reset batch
		}
	}

	// Send remaining messages in the batch
	if len(batch) > 0 {
		if err := tp.sendBatch(batch); err != nil {
			return successCount, fmt.Errorf("failed to send final batch: %w", err)
		}
		successCount += len(batch)
	}

	tp.logger.Info("Worker completed data generation",
		zap.Int("worker_id", workerID),
		zap.Int("records_sent", successCount),
	)

	return successCount, nil
}

// sendBatch sends a batch of messages to Kafka
func (tp *TestProducer) sendBatch(batch []*sarama.ProducerMessage) error {
	for _, message := range batch {
		_, _, err := tp.producer.SendMessage(message)
		if err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}
	}
	return nil
}

// generateTestRecord creates a test SeedStatus record
func (tp *TestProducer) generateTestRecord(workerID, recordIndex int) map[string]interface{} {
	now := time.Now().UnixMilli()
	
	statuses := []string{"COMPLETED", "PROCESSING", "FAILED", "PENDING", "CANCELLED"}
	responses := []string{
		"Operation completed successfully",
		"Processing in progress",
		"Operation failed due to timeout",
		"Waiting for resources",
		"Operation cancelled by user",
	}

	return map[string]interface{}{
		"dateTimestamp":   now,
		"response":        responses[rand.Intn(len(responses))],
		"status":          statuses[rand.Intn(len(statuses))],
		"seedId":          rand.Intn(10000) + 1,
		"subScenarioId":   rand.Intn(100) + 1,
		"scenarioId":      rand.Intn(50) + 1,
		"scriptId":        workerID*1000 + recordIndex,
	}
}

// Close closes the producer
func (tp *TestProducer) Close() error {
	if err := tp.producer.Close(); err != nil {
		return fmt.Errorf("failed to close producer: %w", err)
	}
	tp.logger.Info("Test producer closed successfully")
	return nil
}