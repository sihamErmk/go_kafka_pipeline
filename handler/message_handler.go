package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go_kafka_pipeline/consumer"
	"go_kafka_pipeline/database"
	"go_kafka_pipeline/metrics"
	"go_kafka_pipeline/models"
	"go_kafka_pipeline/pkgavro"

	"go.uber.org/zap"
)

// MessageHandler processes Kafka messages and writes to database
type MessageHandler struct {
	dbWriter     *internaldatabase.PostgresWriter
	deserializer *pkgavro.Deserializer
	metrics      *internalmetrics.Metrics
	logger       *zap.Logger
}

// NewMessageHandler creates a new message handler
func NewMessageHandler(dbWriter *internaldatabase.PostgresWriter, deserializer *pkgavro.Deserializer, metrics *internalmetrics.Metrics, logger *zap.Logger) *MessageHandler {
	return &MessageHandler{
		dbWriter:     dbWriter,
		deserializer: deserializer,
		metrics:      metrics,
		logger:       logger,
	}
}

// ProcessBatch implements internalconsumer.MessageHandler interface
func (h *MessageHandler) ProcessBatch(ctx context.Context, messages []internalconsumer.Message) error {
	if len(messages) == 0 {
		return nil
	}

	startTime := time.Now()
	records := make([]internaldatabase.SeedStatusRecord, 0, len(messages))
	validMessages := 0

	// Process each message in the batch
	for _, msg := range messages {
		record, err := h.processMessage(msg)
		if err != nil {
			h.logger.Warn("Failed to process message, skipping",
				zap.Error(err),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
			)
			continue
		}

		records = append(records, *record)
		validMessages++
	}

	if len(records) == 0 {
		h.logger.Warn("No valid messages in batch, skipping database write")
		return nil
	}

	// Write batch to database
	if err := h.dbWriter.WriteBatch(ctx, records); err != nil {
		h.metrics.RecordDatabaseError()
		return fmt.Errorf("failed to write batch to database: %w", err)
	}

	// Record metrics
	duration := time.Since(startTime)
	h.metrics.RecordKafkaConsumption(validMessages, duration.Seconds())
	h.metrics.RecordDatabaseWrite(len(records), duration.Seconds())

	h.logger.Info("Message batch processed successfully",
		zap.Int("total_messages", len(messages)),
		zap.Int("valid_messages", validMessages),
		zap.Int("records_written", len(records)),
		zap.Duration("duration", duration),
	)

	return nil
}

// processMessage converts a single Kafka message to database record
func (h *MessageHandler) processMessage(msg internalconsumer.Message) (*internaldatabase.SeedStatusRecord, error) {
	// For now, assume messages are JSON (in production, they would be Avro binary)
	var avroData internalmodels.SeedStatusAvro
	if err := json.Unmarshal(msg.Value, &avroData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Convert to database record
	seedStatus := avroData.ToSeedStatus()

	return &internaldatabase.SeedStatusRecord{
		EventTimestamp:  seedStatus.EventTimestamp,
		ResponseMessage: seedStatus.ResponseMessage,
		Status:          seedStatus.Status,
		SeedID:          seedStatus.SeedID,
		SubScenarioID:   seedStatus.SubScenarioID,
		ScenarioID:      seedStatus.ScenarioID,
		ScriptID:        seedStatus.ScriptID,
	}, nil
}
