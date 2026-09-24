package com.chronoweave.shared.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BackoffUtilTest {

    @Test
    void testBackoffGrowthAndJitter() {
        long attempt1 = BackoffUtil.calculateBackoffWithJitter(1, 1000, 2.0);
        // Attempt 1: 1000 * 2^0 * (0.8 ~ 1.2) -> 800 ~ 1200
        assertTrue(attempt1 >= 800 && attempt1 <= 1200, "Attempt 1 out of bounds: " + attempt1);

        long attempt2 = BackoffUtil.calculateBackoffWithJitter(2, 1000, 2.0);
        // Attempt 2: 1000 * 2^1 * (0.8 ~ 1.2) -> 1600 ~ 2400
        assertTrue(attempt2 >= 1600 && attempt2 <= 2400, "Attempt 2 out of bounds: " + attempt2);

        long attempt3 = BackoffUtil.calculateBackoffWithJitter(3, 1000, 2.0);
        // Attempt 3: 1000 * 2^2 * (0.8 ~ 1.2) -> 3200 ~ 4800
        assertTrue(attempt3 >= 3200 && attempt3 <= 4800, "Attempt 3 out of bounds: " + attempt3);
    }
}
