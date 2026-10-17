CREATE TABLE IF NOT EXISTS etl_runs (
    run_id BIGSERIAL PRIMARY KEY,
    start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    end_time TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    extracted_count INTEGER DEFAULT 0,
    valid_count INTEGER DEFAULT 0,
    invalid_count INTEGER DEFAULT 0,
    loaded_count INTEGER DEFAULT 0,
    duration_seconds DOUBLE PRECISION,
    error_message TEXT,
    CONSTRAINT chk_etl_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED'))
);