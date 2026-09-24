package com.chronoweave.shared.exception;

public class DagCycleException extends RuntimeException {
    public DagCycleException(String message) {
        super(message);
    }
}
