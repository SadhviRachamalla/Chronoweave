package com.chronoweave.shared.enums;

public enum JobState {
    PENDING,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    RETRYING,
    DEAD_LETTERED,
    CANCELLED;

    public boolean canTransitionTo(JobState nextState) {
        return switch (this) {
            case PENDING -> nextState == QUEUED || nextState == CANCELLED;
            case QUEUED -> nextState == RUNNING || nextState == CANCELLED;
            case RUNNING -> nextState == SUCCEEDED || nextState == FAILED || nextState == RETRYING || nextState == DEAD_LETTERED || nextState == CANCELLED;
            case RETRYING -> nextState == QUEUED || nextState == DEAD_LETTERED || nextState == CANCELLED;
            case DEAD_LETTERED -> nextState == QUEUED || nextState == CANCELLED;
            case SUCCEEDED, FAILED, CANCELLED -> false;
        };
    }
}
