package whylogs.core.metrics;

import lombok.Data;

@Data
public class OperationResult {
  private final int successes;
  private final int failures;

  public static OperationResult ok(int successes) {
    return new OperationResult(successes, 0);
  }

  public static OperationResult ok() {
    return new OperationResult(1, 0);
  }

  public static OperationResult failed(int failures) {
    return new OperationResult(0, failures);
  }

  public static OperationResult failed() {
    return new OperationResult(0, 1);
  }

  public OperationResult add(OperationResult other) {
    int new_successes = this.successes + other.getSuccesses();
    int new_failures = this.failures + other.getFailures();
    return new OperationResult(new_successes, new_failures);
  }
}
