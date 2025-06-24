-- Initialize the database with optimized settings for high-performance inserts

-- Create the main table
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

CREATE INDEX IF NOT EXISTS idx_processed_seed_status_seed_id 
    ON processed_seed_status(seed_id);

-- Create composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_processed_seed_status_composite 
    ON processed_seed_status(status, event_timestamp);

-- Optimize PostgreSQL settings for bulk operations
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;

-- Apply the settings (requires restart in production)
SELECT pg_reload_conf();

-- Create a function to get table statistics
CREATE OR REPLACE FUNCTION get_table_stats()
RETURNS TABLE(
    table_name TEXT,
    row_count BIGINT,
    table_size TEXT,
    index_size TEXT,
    total_size TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'processed_seed_status'::TEXT,
        (SELECT COUNT(*) FROM processed_seed_status),
        pg_size_pretty(pg_total_relation_size('processed_seed_status'::regclass) - pg_indexes_size('processed_seed_status'::regclass)),
        pg_size_pretty(pg_indexes_size('processed_seed_status'::regclass)),
        pg_size_pretty(pg_total_relation_size('processed_seed_status'::regclass));
END;
$$ LANGUAGE plpgsql;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- Log the initialization
DO $$
BEGIN
    RAISE NOTICE 'Database initialized successfully for high-performance Kafka pipeline';
    RAISE NOTICE 'Table: processed_seed_status created with optimized indexes';
    RAISE NOTICE 'Sequence: processed_seed_status_seq created with cache=500';
    RAISE NOTICE 'Performance settings applied';
END $$;