package com.whylogs.core.metrics.aggregators;

import com.whylogs.core.errors.UnsupportedError;

import java.util.function.BiFunction;

public interface IAggregator<T, U, R> extends BiFunction<T, U, R> {
    R merge(T lhs, U rhs) throws UnsupportedError;
}
