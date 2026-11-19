-- ============================================================================
-- DAS 839 – NoSQL Systems (End Semester Project)
-- Member 4 Deliverable: PostgreSQL Relational Schema for ETL Results & Metadata
-- ============================================================================
-- Specification Requirements (Page 2 & Page 3):
-- 1. Must store aggregated output of Query 1, Query 2, and Query 3 in PostgreSQL.
-- 2. May use one result table per query (chosen here for strict SQL typing).
-- 3. Every stored result MUST include:
--      - pipeline_name   ('mongodb', 'mapreduce', 'pig', 'hive')
--      - run_id          (unique identifier for the execution run)
--      - batch_id        (sequential 1..k for per-batch results; 0 for full-run)
--      - executed_at     (timestamp of execution)
-- 4. Must store execution metadata so the reporting module can display:
--      - selected pipeline, query name, runtime, batch ID, batch size,
--        average batch size, and malformed-record count.
-- ============================================================================

-- 1. Run-Level Execution Metadata Table (1 row per pipeline execution run)
CREATE TABLE IF NOT EXISTS etl_run_metadata (
    run_id                  VARCHAR(64) PRIMARY KEY,
    pipeline_name           VARCHAR(32) NOT NULL CHECK (pipeline_name IN ('mongodb', 'mapreduce', 'pig', 'hive')),
    input_dataset           VARCHAR(255) NOT NULL,
    configured_batch_size   INTEGER NOT NULL CHECK (configured_batch_size > 0),
    total_batches           INTEGER NOT NULL CHECK (total_batches >= 1),
    total_records_processed BIGINT NOT NULL CHECK (total_records_processed >= 0),
    valid_records_count     BIGINT NOT NULL CHECK (valid_records_count >= 0),
    malformed_record_count  BIGINT NOT NULL CHECK (malformed_record_count >= 0),
    avg_batch_size          NUMERIC(14, 2) NOT NULL,
    runtime_seconds         NUMERIC(12, 4) NOT NULL,
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Batch-Level Execution Metadata Table (1 row per sequential batch: batch_id = 1..k)
CREATE TABLE IF NOT EXISTS etl_batch_metadata (
    run_id                  VARCHAR(64) NOT NULL REFERENCES etl_run_metadata(run_id) ON DELETE CASCADE,
    pipeline_name           VARCHAR(32) NOT NULL,
    batch_id                INTEGER NOT NULL CHECK (batch_id >= 1),
    batch_size              INTEGER NOT NULL CHECK (batch_size > 0),
    valid_records           INTEGER NOT NULL CHECK (valid_records >= 0),
    malformed_records       INTEGER NOT NULL CHECK (malformed_records >= 0),
    batch_runtime_seconds   NUMERIC(12, 4) NOT NULL,
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, batch_id)
);

-- 3. Mandatory Query 1 Table: Daily Traffic Summary
-- Required Output Columns: log_date, status_code, request_count, total_bytes
CREATE TABLE IF NOT EXISTS q1_daily_traffic_summary (
    run_id                  VARCHAR(64) NOT NULL REFERENCES etl_run_metadata(run_id) ON DELETE CASCADE,
    pipeline_name           VARCHAR(32) NOT NULL,
    batch_id                INTEGER NOT NULL,  -- 1..k for individual batch, 0 for full-run aggregation
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    log_date                DATE NOT NULL,
    status_code             INTEGER NOT NULL,
    request_count           BIGINT NOT NULL,
    total_bytes             BIGINT NOT NULL,
    PRIMARY KEY (run_id, batch_id, log_date, status_code)
);

-- 4. Mandatory Query 2 Table: Top Requested Resources (Top 20)
-- Required Output Columns: resource_path, request_count, total_bytes, distinct_host_count
CREATE TABLE IF NOT EXISTS q2_top_requested_resources (
    run_id                  VARCHAR(64) NOT NULL REFERENCES etl_run_metadata(run_id) ON DELETE CASCADE,
    pipeline_name           VARCHAR(32) NOT NULL,
    batch_id                INTEGER NOT NULL,  -- 1..k for individual batch, 0 for full-run aggregation
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resource_path           TEXT NOT NULL,
    request_count           BIGINT NOT NULL,
    total_bytes             BIGINT NOT NULL,
    distinct_host_count     BIGINT NOT NULL,
    PRIMARY KEY (run_id, batch_id, resource_path)
);

-- 5. Mandatory Query 3 Table: Hourly Error Analysis (Status Codes 400-599)
-- Required Output Columns: log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
CREATE TABLE IF NOT EXISTS q3_hourly_error_analysis (
    run_id                  VARCHAR(64) NOT NULL REFERENCES etl_run_metadata(run_id) ON DELETE CASCADE,
    pipeline_name           VARCHAR(32) NOT NULL,
    batch_id                INTEGER NOT NULL,  -- 1..k for individual batch, 0 for full-run aggregation
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    log_date                DATE NOT NULL,
    log_hour                SMALLINT NOT NULL CHECK (log_hour BETWEEN 0 AND 23),
    error_request_count     BIGINT NOT NULL,
    total_request_count     BIGINT NOT NULL,
    error_rate              NUMERIC(10, 6) NOT NULL,
    distinct_error_hosts    BIGINT NOT NULL,
    PRIMARY KEY (run_id, batch_id, log_date, log_hour)
);

-- Indexes for fast reporting queries in reporter.py
CREATE INDEX IF NOT EXISTS idx_q1_run_batch ON q1_daily_traffic_summary (run_id, batch_id);
CREATE INDEX IF NOT EXISTS idx_q2_run_batch ON q2_top_requested_resources (run_id, batch_id, request_count DESC);
CREATE INDEX IF NOT EXISTS idx_q3_run_batch ON q3_hourly_error_analysis (run_id, batch_id);
