package com.whylogs.core.errors;

/**
 * Exception when an error happens during logging.
 */
public class LoggingError extends Error {
  public LoggingError(String message) {
    super(message);
  }
}
