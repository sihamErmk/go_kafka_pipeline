package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// KafkaConsumer handles high-performance Kafka message consumption
type KafkaConsumer struct {
	brokers       []string
	topic         string
	groupID       string
	batchSize     int
	workerCount   int
	logger        *zap.Logger
	messageHandler MessageHandler
	consumerGroup sarama.ConsumerGroup
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// MessageHandler defines the interface for processing message batches
type MessageHandler interface {
	ProcessBatch(ctx context.Context, messages []Message) error
}

// Message represents a Kafka message
type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

// Config holds Kafka consumer configuration
type Config struct {
	Brokers       []string
	Topic         string
	GroupID       string
	BatchSize     int
	WorkerCount   int
	SessionTimeout time.Duration
	HeartbeatInterval time.Duration
}

// NewKafkaConsumer creates a new high-performance Kafka consumer
func NewKafkaConsumer(config Config, handler MessageHandler, logger *zap.Logger) (*KafkaConsumer, error) {
	// Configure Sarama for high throughput
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V3_0_0_0
	saramaConfig.Consumer.Group.Session.Timeout = config.SessionTimeout
	saramaConfig.Consumer.Group.Heartbeat.Interval = config.HeartbeatInterval
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	
	// Optimize for batch processing
	saramaConfig.Consumer.Fetch.Max = int32(config.BatchSize)
	saramaConfig.Consumer.MaxProcessingTime = 10 * time.Minute
	saramaConfig.Consumer.Fetch.Default = 50 * 1024 * 1024 // 50MB
	saramaConfig.Consumer.MaxWaitTime = 50 * time.Millisecond

	consumerGroup, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &KafkaConsumer{
		brokers:       config.Brokers,
		topic:         config.Topic,
		groupID:       config.GroupID,
		batchSize:     config.BatchSize,
		workerCount:   config.WorkerCount,
		logger:        logger,
		messageHandler: handler,
		consumerGroup: consumerGroup,
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// Start begins consuming messages with multiple workers
func (kc *KafkaConsumer) Start() error {
	kc.logger.Info("Starting Kafka consumer",
		zap.Strings("brokers", kc.brokers),
		zap.String("topic", kc.topic),
		zap.String("group_id", kc.groupID),
		zap.Int("worker_count", kc.workerCount),
		zap.Int("batch_size", kc.batchSize),
	)

	// Start multiple consumer workers for parallel processing
	for i := 0; i < kc.workerCount; i++ {
		kc.wg.Add(1)
		go kc.consumeWorker(i)
	}

	// Monitor consumer group errors
	go func() {
		for err := range kc.consumerGroup.Errors() {
			kc.logger.Error("Consumer group error", zap.Error(err))
		}
	}()

	return nil
}

// consumeWorker runs a single consumer worker
func (kc *KafkaConsumer) consumeWorker(workerID int) {
	defer kc.wg.Done()

	kc.logger.Info("Starting consumer worker", zap.Int("worker_id", workerID))

	for {
		select {
		case <-kc.ctx.Done():
			kc.logger.Info("Consumer worker stopping", zap.Int("worker_id", workerID))
			return
		default:
			handler := &ConsumerGroupHandler{
				consumer:    kc,
				workerID:    workerID,
				batchBuffer: make([]Message, 0, kc.batchSize),
			}

			err := kc.consumerGroup.Consume(kc.ctx, []string{kc.topic}, handler)
			if err != nil {
				kc.logger.Error("Consumer error", zap.Int("worker_id", workerID), zap.Error(err))
				time.Sleep(time.Second) // Brief pause before retry
			}
		}
	}
}

// Stop gracefully shuts down the consumer
func (kc *KafkaConsumer) Stop() error {
	kc.logger.Info("Stopping Kafka consumer")
	
	kc.cancel()
	kc.wg.Wait()
	
	if err := kc.consumerGroup.Close(); err != nil {
		return fmt.Errorf("failed to close consumer group: %w", err)
	}

	kc.logger.Info("Kafka consumer stopped successfully")
	return nil
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler for batch processing
type ConsumerGroupHandler struct {
	consumer    *KafkaConsumer
	workerID    int
	batchBuffer []Message
	mu          sync.Mutex
}

// Setup is called when a new session is established
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	h.consumer.logger.Info("Consumer session setup", zap.Int("worker_id", h.workerID))
	return nil
}

// Cleanup is called when the session is ending
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.consumer.logger.Info("Consumer session cleanup", zap.Int("worker_id", h.workerID))
	return nil
}

// ConsumeClaim processes messages from a partition
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batchTimer := time.NewTimer(100 * time.Millisecond) // Process batch every 100ms
	defer batchTimer.Stop()

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			h.mu.Lock()
			h.batchBuffer = append(h.batchBuffer, Message{
				Key:       message.Key,
				Value:     message.Value,
				Topic:     message.Topic,
				Partition: message.Partition,
				Offset:    message.Offset,
				Timestamp: message.Timestamp,
			})

			// Process batch when it reaches the configured size
			if len(h.batchBuffer) >= h.consumer.batchSize {
				if err := h.processBatch(session); err != nil {
					h.consumer.logger.Error("Failed to process batch", 
						zap.Int("worker_id", h.workerID), 
						zap.Error(err))
				}
			}
			h.mu.Unlock()

		case <-batchTimer.C:
			// Process partial batch on timer
			h.mu.Lock()
			if len(h.batchBuffer) > 0 {
				if err := h.processBatch(session); err != nil {
					h.consumer.logger.Error("Failed to process timer batch", 
						zap.Int("worker_id", h.workerID), 
						zap.Error(err))
				}
			}
			h.mu.Unlock()
			batchTimer.Reset(100 * time.Millisecond)

		case <-session.Context().Done():
			return nil
		}
	}
}

// processBatch handles a batch of messages
func (h *ConsumerGroupHandler) processBatch(session sarama.ConsumerGroupSession) error {
	if len(h.batchBuffer) == 0 {
		return nil
	}

	startTime := time.Now()
	batchSize := len(h.batchBuffer)

	h.consumer.logger.Info("Processing message batch",
		zap.Int("worker_id", h.workerID),
		zap.Int("batch_size", batchSize),
	)

	// Process the batch
	if err := h.consumer.messageHandler.ProcessBatch(session.Context(), h.batchBuffer); err != nil {
		return fmt.Errorf("message handler failed: %w", err)
	}

	// Mark messages as processed
	for _, msg := range h.batchBuffer {
		session.MarkMessage(&sarama.ConsumerMessage{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}, "")
	}

	duration := time.Since(startTime)
	throughput := float64(batchSize) / duration.Seconds()

	h.consumer.logger.Info("Batch processed successfully",
		zap.Int("worker_id", h.workerID),
		zap.Int("batch_size", batchSize),
		zap.Duration("duration", duration),
		zap.Float64("throughput_per_sec", throughput),
	)

	// Clear the batch buffer
	h.batchBuffer = h.batchBuffer[:0]
	return nil
}