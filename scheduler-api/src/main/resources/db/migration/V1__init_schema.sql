-- Initial Database Schema for Chronoweave

CREATE TABLE jobs (
    id VARCHAR(36) PRIMARY KEY,
    idempotency_key VARCHAR(128) UNIQUE,
    name VARCHAR(255) NOT NULL,
    job_type VARCHAR(64) NOT NULL,
    payload TEXT,
    priority INT NOT NULL DEFAULT 0,
    effective_priority INT NOT NULL DEFAULT 0,
    state VARCHAR(32) NOT NULL,
    required_capability VARCHAR(64) NOT NULL DEFAULT 'DEFAULT',
    max_attempts INT NOT NULL DEFAULT 3,
    current_attempt INT NOT NULL DEFAULT 0,
    retry_backoff_ms BIGINT NOT NULL DEFAULT 1000,
    assigned_worker_id VARCHAR(128),
    scheduled_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE job_executions (
    id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INT NOT NULL,
    worker_id VARCHAR(128) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(32) NOT NULL,
    error_message TEXT,
    output_data TEXT
);

CREATE TABLE job_dependencies (
    job_id VARCHAR(36) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    parent_job_id VARCHAR(36) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, parent_job_id)
);

CREATE INDEX idx_jobs_state_effective_priority ON jobs(state, effective_priority DESC);
CREATE INDEX idx_jobs_idempotency ON jobs(idempotency_key);
CREATE INDEX idx_job_executions_job_id ON job_executions(job_id);
