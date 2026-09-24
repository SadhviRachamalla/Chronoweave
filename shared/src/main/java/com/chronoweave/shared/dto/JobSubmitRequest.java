package com.chronoweave.shared.dto;

import com.chronoweave.shared.enums.JobType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public record JobSubmitRequest(
    String idempotencyKey,
    @NotBlank(message = "Job name is required") String name,
    @NotNull(message = "Job type is required") JobType jobType,
    Map<String, Object> payload,
    @Min(0) @Max(100) int priority,
    String requiredCapability,
    @Min(1) @Max(10) Integer maxAttempts,
    @Min(100) Long retryBackoffMs,
    List<String> dependsOnJobIds
) {
    public JobSubmitRequest {
        if (requiredCapability == null || requiredCapability.isBlank()) {
            requiredCapability = "DEFAULT";
        }
        if (maxAttempts == null) {
            maxAttempts = 3;
        }
        if (retryBackoffMs == null) {
            retryBackoffMs = 1000L;
        }
        if (dependsOnJobIds == null) {
            dependsOnJobIds = List.of();
        }
    }
}
