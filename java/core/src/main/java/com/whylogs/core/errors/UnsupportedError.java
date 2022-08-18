package com.whylogs.core.errors;

/** * Exception raised when an operation is not supported. */
public class UnsupportedError extends Error {
  public UnsupportedError(String message) {
    super(message);
  }
}
