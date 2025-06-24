package main

import (
	"context"
	"fmt"
	consumer "go_kafka_pipeline/internalconsumer"
	database "go_kafka_pipeline/internaldatabase"
	handler "go_kafka_pipeline/internalhandler"
	metrics "go_kafka_pipeline/internalmetrics"
	avro "go_kafka_pipeline/pkg/avro"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: .env file not found: %v", err)
	}

	// Initialize logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Initialize configuration
	cfg := &Config{
		KafkaBrokers:    getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "seed-status-updates"),
		KafkaGroupID:    getEnv("KAFKA_GROUP_ID", "postgres-inserter-group-go"),
		DatabaseURL:     getEnv("DB_URL", "postgres://postgres:password@localhost:5432/processed_data_db?sslmode=disable"),
		ServerPort:      getEnv("SERVER_PORT", "8080"),
		ConsumerWorkers: 20,
		BatchSize:       2500,
	}

	logger.Info("Starting Kafka-PostgreSQL Pipeline",
		zap.String("kafka_brokers", cfg.KafkaBrokers),
		zap.String("kafka_topic", cfg.KafkaTopic),
		zap.String("server_port", cfg.ServerPort),
		zap.Int("consumer_workers", cfg.ConsumerWorkers),
	)

	// Initialize components
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup HTTP server with metrics and API endpoints
	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/health", healthHandler).Methods("GET")
	router.HandleFunc("/send-test-data-concurrently", sendTestDataHandler).Methods("POST")
	router.HandleFunc("/tracking-status", trackingStatusHandler).Methods("GET")
	router.HandleFunc("/reset-tracking", resetTrackingHandler).Methods("POST")

	server := &http.Server{
		Addr:    ":" + cfg.ServerPort,
		Handler: router,
	}

	// Start HTTP server
	go func() {
		logger.Info("Starting HTTP server", zap.String("port", cfg.ServerPort))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	// Initialize components
	appMetrics := metrics.NewMetrics()

	// Initialize database writer
	dbWriter, err := database.NewPostgresWriter(cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatal("Failed to initialize database writer", zap.Error(err))
	}
	defer dbWriter.Close()

	// Create database table
	if err := dbWriter.CreateTable(ctx); err != nil {
		logger.Fatal("Failed to create database table", zap.Error(err))
	}

	// Initialize Avro deserializer
	deserializer, err := avro.NewDeserializer("schema/seed_status.avsc")
	if err != nil {
		logger.Fatal("Failed to initialize Avro deserializer", zap.Error(err))
	}

	// Initialize message handler
	messageHandler := handler.NewMessageHandler(dbWriter, deserializer, appMetrics, logger)

	// Initialize Kafka consumer
	consumerConfig := consumer.Config{
		Brokers:           []string{cfg.KafkaBrokers},
		Topic:             cfg.KafkaTopic,
		GroupID:           cfg.KafkaGroupID,
		BatchSize:         cfg.BatchSize,
		WorkerCount:       cfg.ConsumerWorkers,
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 3 * time.Second,
	}

	kafkaConsumer, err := consumer.NewKafkaConsumer(consumerConfig, messageHandler, logger)
	if err != nil {
		logger.Fatal("Failed to initialize Kafka consumer", zap.Error(err))
	}

	// Start Kafka consumer
	if err := kafkaConsumer.Start(); err != nil {
		logger.Fatal("Failed to start Kafka consumer", zap.Error(err))
	}
	defer kafkaConsumer.Stop()

	// Set initial metrics
	appMetrics.SetActiveWorkers(cfg.ConsumerWorkers)

	// Start metrics monitoring goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := dbWriter.GetStats()
				appMetrics.SetDatabaseConnections(int(stats.AcquiredConns()))
			}
		}
	}()

	logger.Info("Application started successfully")

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Shutting down application...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	cancel() // Cancel context for all goroutines

	// Stop Kafka consumer gracefully
	logger.Info("Stopping Kafka consumer...")
	if err := kafkaConsumer.Stop(); err != nil {
		logger.Error("Error stopping Kafka consumer", zap.Error(err))
	} else {
		logger.Info("Kafka consumer stopped successfully")
	}

	wg.Wait() // Wait for all workers to finish

	logger.Info("Application shutdown complete")
}

type Config struct {
	KafkaBrokers    string
	KafkaTopic      string
	KafkaGroupID    string
	DatabaseURL     string
	ServerPort      string
	ConsumerWorkers int
	BatchSize       int
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// HTTP Handlers (placeholder implementations)
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"healthy","timestamp":"%s"}`, time.Now().Format(time.RFC3339))
}

func sendTestDataHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Parse query parameters
	count := 10000 // default
	if countStr := r.URL.Query().Get("count"); countStr != "" {
		if parsedCount, err := fmt.Sscanf(countStr, "%d", &count); err != nil || parsedCount != 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, `{"error":"Invalid count parameter"}`)
			return
		}
	}

	// This would integrate with the test producer in a real implementation
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"message":"Test data generation started","count":%d}`, count)
}

func trackingStatusHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement processing tracking
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"processed_records":0,"total_time_ms":0,"throughput":0}`)
}

func resetTrackingHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement tracking reset
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"message":"Tracking reset not implemented yet"}`)
}
