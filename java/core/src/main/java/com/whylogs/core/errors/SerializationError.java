package com.whylogs.core.errors;

/** Exception raised when serializing data. */
public class SerializationError extends Error {
  public SerializationError(String message) {
    super(message);
  }
}
