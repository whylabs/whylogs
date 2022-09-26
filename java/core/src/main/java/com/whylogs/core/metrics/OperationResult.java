package com.whylogs.core.metrics;

import lombok.Data;
import lombok.Getter;

@Data
@Getter
public class OperationResult {
  private final int successes;
  private final int failures;
  private final int nulls;

  public static OperationResult nulls(int numberOfNulls){
    return new OperationResult(0, 0, numberOfNulls);
  }

  public static OperationResult status(int successes, int failures, int nulls){
    return new OperationResult(successes, failures, nulls);
  }

  public static OperationResult ok(int successes) {
    return new OperationResult(successes, 0, 0);
  }

  public static OperationResult ok() {
    return new OperationResult(1, 0, 0);
  }

  public static OperationResult failed(int failures) {
    return new OperationResult(0, failures, 0);
  }

  public static OperationResult failed() {
    return new OperationResult(0, 1, 0);
  }

  public OperationResult add(OperationResult other) {
    int new_successes = this.successes + other.getSuccesses();
    int new_failures = this.failures + other.getFailures();
    int new_nulls = this.nulls + other.getNulls();
    return new OperationResult(new_successes, new_failures, new_nulls);
  }
}
