package com.chronoweave.shared.dto;

import com.chronoweave.shared.enums.JobState;
import com.chronoweave.shared.enums.JobType;
import java.time.Instant;
import java.util.List;

public record JobResponse(
    String id,
    String idempotencyKey,
    String name,
    JobType jobType,
    String payload,
    int priority,
    int effectivePriority,
    JobState state,
    String requiredCapability,
    int maxAttempts,
    int currentAttempt,
    long retryBackoffMs,
    String assignedWorkerId,
    Instant scheduledAt,
    Instant createdAt,
    Instant updatedAt,
    List<String> dependsOnJobIds
) {}
