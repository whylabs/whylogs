package com.whylogs.core.metrics.serializers;

import whylogs.core.message.MetricComponentMessage;

import java.util.function.Function;

public interface ISerialization<T> extends Function<T, MetricComponentMessage> {
    public MetricComponentMessage serialize(T value);
}
