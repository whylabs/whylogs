package com.whylogs.core.metrics.aggregators;

import com.whylogs.core.errors.UnsupportedError;
import lombok.SneakyThrows;

import java.util.function.BiFunction;

public interface Aggregator<T> extends BiFunction<T, T, T> {
    @SneakyThrows
    T apply(T lhs, T rhs);

    T merge(T lhs, T rhs) throws UnsupportedError;
}
