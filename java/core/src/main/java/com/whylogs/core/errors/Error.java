package com.whylogs.core.errors;

/** Base error type for this module. */
public class Error extends Exception {
  public Error(String message) {
    super(message);
  }
}
