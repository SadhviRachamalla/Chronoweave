package com.chronoweave.shared.dto;

import java.time.Instant;

public record JobExecutionResponse(
    Long id,
    String jobId,
    int attempt,
    String workerId,
    Instant startedAt,
    Instant completedAt,
    String status,
    String errorMessage,
    String outputData
) {}
