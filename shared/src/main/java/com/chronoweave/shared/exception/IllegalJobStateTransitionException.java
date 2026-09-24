package com.chronoweave.shared.exception;

public class IllegalJobStateTransitionException extends RuntimeException {
    public IllegalJobStateTransitionException(String message) {
        super(message);
    }
}
