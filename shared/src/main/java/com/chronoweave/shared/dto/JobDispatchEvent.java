package com.chronoweave.shared.dto;

import com.chronoweave.shared.enums.JobType;

public record JobDispatchEvent(
    String jobId,
    JobType jobType,
    String payload,
    String requiredCapability,
    int attempt,
    long retryBackoffMs
) {}
