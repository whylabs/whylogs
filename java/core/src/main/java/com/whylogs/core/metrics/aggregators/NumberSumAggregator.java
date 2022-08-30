package com.whylogs.core.metrics.aggregators;

import com.whylogs.core.errors.UnsupportedError;
import lombok.SneakyThrows;

// This sum forces lhs, rhs, and result to be the same type
public class NumberSumAggregator<T extends Number> implements IAggregator<T> {

  @Override
  public T merge(T lhs, T rhs) throws UnsupportedError {
    if (lhs instanceof Double) {
      Double result = lhs.doubleValue() + rhs.doubleValue();
      return (T) result;
    }

    if (lhs instanceof Float) {
      Float result = lhs.floatValue() + rhs.floatValue();
      return (T) result;
    }

    if (lhs instanceof Long) {
      Long result = lhs.longValue() + rhs.longValue();
      return (T) result;
    }

    if (lhs instanceof Integer) {
      Integer result = lhs.intValue() + rhs.intValue();
      return (T) result;
    }

    if (lhs instanceof Short) {
      Short result = (short) (lhs.shortValue() + rhs.shortValue());
      return (T) result;
    }

    if (lhs instanceof Byte) {
      Byte result = (byte) (lhs.byteValue() + rhs.byteValue());
      return (T) result;
    }

    throw new UnsupportedError("Unsupported type for sum: " + lhs.getClass().getName());
  }

  @SneakyThrows
  @Override
  public T apply(T lhs, T rhs) {
    return merge(lhs, rhs);
  }
}
