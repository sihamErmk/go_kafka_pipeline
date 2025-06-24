package handler

import (
	"context"
	"fmt"
	"time"

	consumer "go_kafka_pipeline/internalconsumer"
	database "go_kafka_pipeline/internaldatabase"
	metrics "go_kafka_pipeline/internalmetrics"

	avro "go_kafka_pipeline/pkg/avro"

	"go.uber.org/zap"
)

// MessageHandler processes Kafka messages and writes to database
type MessageHandler struct {
	dbWriter     *database.PostgresWriter
	deserializer *avro.Deserializer
	metrics      *metrics.Metrics
	logger       *zap.Logger
}

// NewMessageHandler creates a new message handler
func NewMessageHandler(dbWriter *database.PostgresWriter, deserializer *avro.Deserializer, metrics *metrics.Metrics, logger *zap.Logger) *MessageHandler {
	return &MessageHandler{
		dbWriter:     dbWriter,
		deserializer: deserializer,
		metrics:      metrics,
		logger:       logger,
	}
}

// ProcessBatch implements consumer.MessageHandler interface
func (h *MessageHandler) ProcessBatch(ctx context.Context, messages []consumer.Message) error {
	if len(messages) == 0 {
		return nil
	}

	startTime := time.Now()
	records := make([]database.SeedStatusRecord, 0, len(messages))
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
func (h *MessageHandler) processMessage(msg consumer.Message) (*database.SeedStatusRecord, error) {
	// Deserialize Avro binary data
	data, err := h.deserializer.Deserialize(msg.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Avro: %w", err)
	}

	// Extract fields with type assertions
	timestamp := time.Now()
	if ts, ok := data["dateTimestamp"].(float64); ok {
		timestamp = time.UnixMilli(int64(ts))
	}

	var response *string
	if r, ok := data["response"].(string); ok {
		response = &r
	}

	status := "UNKNOWN"
	if s, ok := data["status"].(string); ok {
		status = s
	}

	var seedID, subScenarioID, scenarioID, scriptID *int32
	if id, ok := data["seedId"].(float64); ok {
		val := int32(id)
		seedID = &val
	}
	if id, ok := data["subScenarioId"].(float64); ok {
		val := int32(id)
		subScenarioID = &val
	}
	if id, ok := data["scenarioId"].(float64); ok {
		val := int32(id)
		scenarioID = &val
	}
	if id, ok := data["scriptId"].(float64); ok {
		val := int32(id)
		scriptID = &val
	}

	return &database.SeedStatusRecord{
		EventTimestamp:  timestamp,
		ResponseMessage: response,
		Status:          status,
		SeedID:          seedID,
		SubScenarioID:   subScenarioID,
		ScenarioID:      scenarioID,
		ScriptID:        scriptID,
	}, nil
}
