#!/usr/bin/env bash
# Chronoweave Demo Script & Integration Test Guide

SCHEDULER_URL="http://localhost:8080"

echo "=== 1. Checking Scheduler Health Endpoint ==="
curl -s "${SCHEDULER_URL}/actuator/health" | grep -q "UP" && echo "Scheduler is UP!" || echo "Scheduler is DOWN"

echo ""
echo "=== 2. Submitting Normal ECHO Job ==="
curl -X POST "${SCHEDULER_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Demo Echo Job",
    "jobType": "ECHO",
    "payload": {"message": "Hello Chronoweave!"},
    "priority": 5
  }'

echo ""
echo "=== 3. Submitting High-Priority Job ==="
curl -X POST "${SCHEDULER_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "High Priority Compute",
    "jobType": "SIMULATED_COMPUTE",
    "priority": 10
  }'

echo ""
echo "=== 4. Submitting Idempotent Duplicate Job ==="
curl -X POST "${SCHEDULER_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "demo-unique-key-101",
    "name": "Idempotent Task",
    "jobType": "ECHO",
    "priority": 1
  }'

echo ""
echo "=== Resubmitting Same Idempotency Key (Should return original job) ==="
curl -X POST "${SCHEDULER_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "demo-unique-key-101",
    "name": "Idempotent Task",
    "jobType": "ECHO",
    "priority": 1
  }'

echo ""
echo "=== 5. Submitting Failing Job for Retry -> DLQ ==="
curl -X POST "${SCHEDULER_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Intentional Failure",
    "jobType": "FAIL_ON_PURPOSE",
    "maxAttempts": 2,
    "retryBackoffMs": 500
  }'

echo ""
echo "=== 6. Listing All Jobs ==="
curl -s "${SCHEDULER_URL}/api/v1/jobs"
