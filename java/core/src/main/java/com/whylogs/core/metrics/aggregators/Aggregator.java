package com.whylogs.core.metrics.aggregators;

import com.whylogs.core.errors.UnsupportedError;
import java.util.function.BiFunction;

public interface Aggregator<T> extends BiFunction<T, T, T> {
  T merge(T lhs, T rhs) throws UnsupportedError;
}
