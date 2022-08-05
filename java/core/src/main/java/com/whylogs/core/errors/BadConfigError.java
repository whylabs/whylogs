package com.whylogs.core.errors;

/**
 * Exception when an error is due to bad configuration.
 */
public class BadConfigError extends Error {
    public BadConfigError(String message) {
        super(message);
    }
}
