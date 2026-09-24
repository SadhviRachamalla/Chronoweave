package com.chronoweave.shared.dto;

import java.util.List;

public record WorkerHeartbeat(
    String workerId,
    List<String> capabilities,
    int maxSlots,
    int activeSlots,
    long timestamp
) {}
