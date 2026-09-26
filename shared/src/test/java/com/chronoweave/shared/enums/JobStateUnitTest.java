package com.chronoweave.shared.enums;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JobStateUnitTest {

    @Test
    void testStateTransitions() {
        assertTrue(JobState.PENDING.canTransitionTo(JobState.QUEUED));
        assertTrue(JobState.PENDING.canTransitionTo(JobState.CANCELLED));
        assertTrue(JobState.QUEUED.canTransitionTo(JobState.RUNNING));
        assertTrue(JobState.RUNNING.canTransitionTo(JobState.SUCCEEDED));
        assertTrue(JobState.RUNNING.canTransitionTo(JobState.FAILED));
        assertTrue(JobState.RUNNING.canTransitionTo(JobState.RETRYING));
        assertTrue(JobState.RUNNING.canTransitionTo(JobState.DEAD_LETTERED));
        assertTrue(JobState.RETRYING.canTransitionTo(JobState.QUEUED));
        assertTrue(JobState.DEAD_LETTERED.canTransitionTo(JobState.QUEUED));

        assertFalse(JobState.PENDING.canTransitionTo(JobState.SUCCEEDED));
        assertFalse(JobState.QUEUED.canTransitionTo(JobState.SUCCEEDED));
        assertFalse(JobState.SUCCEEDED.canTransitionTo(JobState.QUEUED));
        assertFalse(JobState.FAILED.canTransitionTo(JobState.RUNNING));
        assertFalse(JobState.CANCELLED.canTransitionTo(JobState.QUEUED));
    }
}
