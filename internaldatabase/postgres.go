package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// PostgresWriter handles optimized PostgreSQL COPY operations
type PostgresWriter struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// SeedStatusRecord represents a record for COPY operation
type SeedStatusRecord struct {
	EventTimestamp  time.Time
	ResponseMessage *string
	Status          string
	SeedID          *int32
	SubScenarioID   *int32
	ScenarioID      *int32
	ScriptID        *int32
}

// NewPostgresWriter creates a new PostgreSQL writer with connection pool
func NewPostgresWriter(databaseURL string, logger *zap.Logger) (*PostgresWriter, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	// Optimize connection pool for high throughput
	config.MaxConns = 50
	config.MinConns = 10
	config.MaxConnLifetime = 20 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresWriter{
		pool:   pool,
		logger: logger,
	}, nil
}

// WriteBatch performs optimized bulk insert using PostgreSQL COPY
func (pw *PostgresWriter) WriteBatch(ctx context.Context, records []SeedStatusRecord) error {
	if len(records) == 0 {
		return nil
	}

	startTime := time.Now()

	conn, err := pw.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Use COPY for maximum performance
	copyCount, err := conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{"processed_seed_status"},
		[]string{"event_timestamp", "response_message", "status", "seed_id", "sub_scenario_id", "scenario_id", "script_id"},
		pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
			record := records[i]
			return []interface{}{
				record.EventTimestamp,
				record.ResponseMessage,
				record.Status,
				record.SeedID,
				record.SubScenarioID,
				record.ScenarioID,
				record.ScriptID,
			}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("COPY operation failed: %w", err)
	}

	duration := time.Since(startTime)
	throughput := float64(copyCount) / duration.Seconds()

	pw.logger.Info("Batch write completed",
		zap.Int64("records_written", copyCount),
		zap.Duration("duration", duration),
		zap.Float64("throughput_per_sec", throughput),
	)

	return nil
}

// CreateTable creates the processed_seed_status table if it doesn't exist
func (pw *PostgresWriter) CreateTable(ctx context.Context) error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS processed_seed_status (
		id BIGSERIAL PRIMARY KEY,
		event_timestamp TIMESTAMP NOT NULL,
		response_message TEXT,
		status VARCHAR(255) NOT NULL,
		seed_id INTEGER,
		sub_scenario_id INTEGER,
		scenario_id INTEGER,
		script_id INTEGER
	);

	-- Create optimized sequence for high-performance inserts
	CREATE SEQUENCE IF NOT EXISTS processed_seed_status_seq 
		INCREMENT BY 500 
		CACHE 500;

	-- Create indexes for better query performance
	CREATE INDEX IF NOT EXISTS idx_processed_seed_status_timestamp 
		ON processed_seed_status(event_timestamp);
	CREATE INDEX IF NOT EXISTS idx_processed_seed_status_status 
		ON processed_seed_status(status);
	`

	_, err := pw.pool.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	pw.logger.Info("Database table and indexes created successfully")
	return nil
}

// Close closes the connection pool
func (pw *PostgresWriter) Close() {
	pw.pool.Close()
	pw.logger.Info("Database connection pool closed")
}

// GetStats returns connection pool statistics
func (pw *PostgresWriter) GetStats() *pgxpool.Stat {
	return pw.pool.Stat()
}