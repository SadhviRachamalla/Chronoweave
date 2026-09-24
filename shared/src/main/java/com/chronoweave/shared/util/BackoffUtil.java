package com.chronoweave.shared.util;

import java.util.Random;

public class BackoffUtil {
    private static final Random RANDOM = new Random();

    public static long calculateBackoffWithJitter(int attempt, long initialBackoffMs, double multiplier) {
        long rawBackoff = (long) (initialBackoffMs * Math.pow(multiplier, Math.max(0, attempt - 1)));
        // Add +- 20% jitter
        double jitter = 0.8 + (RANDOM.nextDouble() * 0.4);
        return (long) (rawBackoff * jitter);
    }
}
