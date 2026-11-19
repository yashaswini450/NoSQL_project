CREATE TABLE IF NOT EXISTS runs (
    run_id UUID PRIMARY KEY,
    pipeline TEXT NOT NULL CHECK (pipeline IN ('mongodb','mapreduce','pig','hive')),
    status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
    parser_version TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    configured_batch_size BIGINT NOT NULL CHECK (configured_batch_size > 0),
    total_records BIGINT NOT NULL DEFAULT 0,
    valid_records BIGINT NOT NULL DEFAULT 0,
    malformed_records BIGINT NOT NULL DEFAULT 0,
    batch_count INTEGER NOT NULL DEFAULT 0,
    average_batch_size NUMERIC(24,6) NOT NULL DEFAULT 0,
    runtime_seconds NUMERIC(20,6),
    sources JSONB NOT NULL DEFAULT '[]',
    engine_versions JSONB NOT NULL DEFAULT '{}',
    error TEXT,
    UNIQUE (run_id, pipeline)
);
CREATE TABLE IF NOT EXISTS batches (
    run_id UUID NOT NULL REFERENCES runs,
    batch_id INTEGER NOT NULL CHECK (batch_id > 0),
    actual_batch_size BIGINT NOT NULL CHECK (actual_batch_size > 0),
    valid_records BIGINT NOT NULL,
    malformed_records BIGINT NOT NULL,
    malformed_reasons JSONB NOT NULL,
    malformed_examples JSONB NOT NULL,
    elapsed_seconds NUMERIC(20,6) NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (actual_batch_size = valid_records + malformed_records),
    PRIMARY KEY (run_id, batch_id)
);
CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    run_id UUID NOT NULL,
    pipeline TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('BATCH','FINAL')),
    batch_id INTEGER NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    log_date DATE NOT NULL,
    status_code SMALLINT NOT NULL,
    request_count BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    PRIMARY KEY (run_id, scope, batch_id, log_date, status_code),
    FOREIGN KEY (run_id, pipeline) REFERENCES runs(run_id, pipeline),
    FOREIGN KEY (run_id, batch_id) REFERENCES batches
);
CREATE TABLE IF NOT EXISTS q2_top_resources (
    run_id UUID NOT NULL,
    pipeline TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('BATCH','FINAL')),
    batch_id INTEGER NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    rank SMALLINT NOT NULL CHECK (rank BETWEEN 1 AND 20),
    resource_path TEXT NOT NULL,
    request_count BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    distinct_host_count BIGINT NOT NULL,
    PRIMARY KEY (run_id, scope, batch_id, rank),
    FOREIGN KEY (run_id, pipeline) REFERENCES runs(run_id, pipeline),
    FOREIGN KEY (run_id, batch_id) REFERENCES batches
);
CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    run_id UUID NOT NULL,
    pipeline TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('BATCH','FINAL')),
    batch_id INTEGER NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    log_date DATE NOT NULL,
    log_hour SMALLINT NOT NULL CHECK (log_hour BETWEEN 0 AND 23),
    error_request_count BIGINT NOT NULL,
    total_request_count BIGINT NOT NULL,
    error_rate NUMERIC(20,12) NOT NULL,
    distinct_error_hosts BIGINT NOT NULL,
    PRIMARY KEY (run_id, scope, batch_id, log_date, log_hour),
    FOREIGN KEY (run_id, pipeline) REFERENCES runs(run_id, pipeline),
    FOREIGN KEY (run_id, batch_id) REFERENCES batches
);
