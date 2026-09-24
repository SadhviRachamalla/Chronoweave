# Chronoweave Architecture & System Design Document

## 1. Overview
Chronoweave is a high-performance, fault-tolerant Distributed Job Scheduling & Workflow Platform built with Java 21, Spring Boot 3, PostgreSQL, Apache Kafka, and Redis.

## 2. System Architecture
```
                     +---------------------------------------+
                     |             Client / API              |
                     +---------------------------------------+
                                         | REST
                                         v
                     +---------------------------------------+
                     |         scheduler-api (Leader)        |
                     |  - REST Ingestion & Idempotency Check |
                     |  - Redisson Leader Election           |
                     |  - Priority Scheduling & DAG Resolver |
                     |  - Worker Heartbeat Tracker           |
                     +---------------------------------------+
                        |                 |                 |
             PostgreSQL |                 | Redis           | Kafka (Dispatch & Status)
                        v                 v                 v
           +----------------+    +----------------+   +-----------------------+
           | Job DB         |    | Locks, Leader  |   | chronoweave-dispatch  |
           | DAG DB         |    | State & Heart- |   | chronoweave-status   |
           | Audit History  |    | beats          |   | chronoweave-retry     |
           +----------------+    +----------------+   | chronoweave-dlq       |
                                                      +-----------------------+
                                                                  |
                                                                  v
                                                      +-----------------------+
                                                      |   worker (Node 1..N)  |
                                                      |  - Capability Matching|
                                                      |  - Slot Semaphore     |
                                                      |  - Safe Job Executors |
                                                      |  - Heartbeat Publisher|
                                                      +-----------------------+
```

## 3. Database Schema (Flyway V1__init_schema.sql)
### 3.1 `jobs`
- `id` (VARCHAR(36), PK) - UUID string.
- `idempotency_key` (VARCHAR(128), UNIQUE NULLABLE) - Client idempotency token.
- `name` (VARCHAR(255), NOT NULL).
- `job_type` (VARCHAR(64), NOT NULL) - Safe types: `ECHO`, `SLEEP`, `HTTP_CALL`, `SIMULATED_COMPUTE`, `FAIL_ON_PURPOSE`.
- `payload` (TEXT) - JSON parameters for the task.
- `priority` (INT, NOT NULL, DEFAULT 0) - Base priority (higher = processed earlier).
- `effective_priority` (INT, NOT NULL, DEFAULT 0) - Base priority + aging bonus.
- `state` (VARCHAR(32), NOT NULL) - State machine status.
- `required_capability` (VARCHAR(64), NOT NULL, DEFAULT 'DEFAULT').
- `max_attempts` (INT, NOT NULL, DEFAULT 3).
- `current_attempt` (INT, NOT NULL, DEFAULT 0).
- `retry_backoff_ms` (BIGINT, NOT NULL, DEFAULT 1000).
- `assigned_worker_id` (VARCHAR(128), NULLABLE).
- `scheduled_at` (TIMESTAMP WITH TIME ZONE, NULLABLE).
- `created_at` (TIMESTAMP WITH TIME ZONE, NOT NULL).
- `updated_at` (TIMESTAMP WITH TIME ZONE, NOT NULL).

### 3.2 `job_executions` (Audit History)
- `id` (BIGSERIAL, PK).
- `job_id` (VARCHAR(36), NOT NULL, FK to jobs.id).
- `attempt` (INT, NOT NULL).
- `worker_id` (VARCHAR(128), NOT NULL).
- `started_at` (TIMESTAMP WITH TIME ZONE, NOT NULL).
- `completed_at` (TIMESTAMP WITH TIME ZONE, NULLABLE).
- `status` (VARCHAR(32), NOT NULL).
- `error_message` (TEXT).
- `output_data` (TEXT).

### 3.3 `workflow_dags` & `job_dependencies`
- `workflow_dags` (id VARCHAR(36), name VARCHAR(255), created_at TIMESTAMP).
- `job_dependencies` (job_id VARCHAR(36), parent_job_id VARCHAR(36), PRIMARY KEY(job_id, parent_job_id)).

### 3.4 Indexes
- `idx_jobs_state_effective_priority` on `jobs(state, effective_priority DESC)`.
- `idx_jobs_idempotency` on `jobs(idempotency_key)`.
- `idx_job_executions_job_id` on `job_executions(job_id)`.

## 4. Kafka Design
- **Topics**:
  - `chronoweave-dispatch` (Partitions: 3, Keys: `job_type` / `required_capability`).
  - `chronoweave-status` (Partitions: 3, Keys: `job_id`).
  - `chronoweave-retry` (Partitions: 3, Keys: `job_id`).
  - `chronoweave-dlq` (Partitions: 3, Keys: `job_id`).
- **Consumer Groups**:
  - `chronoweave-worker-group` (Workers consuming `chronoweave-dispatch`).
  - `chronoweave-scheduler-group` (Scheduler consuming status / execution result events).

## 5. Redis Responsibilities & Key Patterns
- **Leader Election Lock**: `chronoweave:leader:lock` (Redisson RLock with TTL lease).
- **Worker Registry & Heartbeats**:
  - Hash: `chronoweave:workers:{worker_id}` -> JSON metadata `{workerId, capabilities, maxSlots, activeSlots, lastHeartbeat}`.
  - Set: `chronoweave:active_workers` -> set of `worker_id` strings.
- **Dispatch Lock**: `chronoweave:job:lock:{job_id}` (Prevents duplicate dispatch).

## 6. Job State Machine
```
                       +-----------+
                       |  PENDING  | (Submitted)
                       +-----------+
                             |
                             v
                       +-----------+
             +-------->|  QUEUED   |<----------------------+
             |         +-----------+                       |
             |               |                             |
             |               v                             |
             |         +-----------+                       |
             |         |  RUNNING  |                       |
             |         +-----------+                       |
             |          /    |    \                        |
             |         /     |     \                       |
       Retry |        v      v      v                      | Requeue
    Backoff  |    +-------+ +-----+ +---------------+      |
             +----|RETRYING||SUCC.| | DEAD_LETTERED |------+
                  +-------+ +-----+ +---------------+
                               |
                               +----> +-----------+
                                      | CANCELLED |
                                      +-----------+
```
- Illegal transitions throw `IllegalJobStateTransitionException`.

## 7. Worker Protocol
1. **Heartbeat**: Every 5 seconds, worker updates `chronoweave:workers:{worker_id}` with expiration TTL = 15 seconds.
2. **Capability & Slot Matching**: Workers listen to dispatch messages, verify capacity (`activeSlots < maxSlots`) & capability match (`capabilities.contains(required_capability)`).
3. **Execution & Idempotent Audit**: Uses atomic state check before starting task execution.
4. **Result Reporting**: Emits completion / failure status to `chronoweave-status` Kafka topic.

## 8. Failure-Recovery Strategy
- **Worker Crash Mid-Job**: Scheduler heartbeat sweeper detects stale worker (no heartbeat > 15s). Any jobs assigned to stale worker in `RUNNING` state are re-queued or marked `RETRYING`.
- **Scheduler Leader Failover**: Multi-node scheduler uses Redisson lock. If leader drops, standby acquires lock and starts polling/aging routines.
- **In-flight Job Recovery on Restart**: Unfinished `QUEUED` or `RUNNING` jobs on restart are inspected and safely recovered.
