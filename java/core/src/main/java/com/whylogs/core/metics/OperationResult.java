package com.whylogs.core.metics;

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
        return new OperationResult(this.successes + other.successes, this.failures + other.failures);
    }
}
