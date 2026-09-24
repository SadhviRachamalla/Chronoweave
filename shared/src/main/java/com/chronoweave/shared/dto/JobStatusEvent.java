package com.chronoweave.shared.dto;

import com.chronoweave.shared.enums.JobState;

public record JobStatusEvent(
    String jobId,
    int attempt,
    String workerId,
    JobState status,
    String errorMessage,
    String outputData,
    long executionDurationMs
) {}
