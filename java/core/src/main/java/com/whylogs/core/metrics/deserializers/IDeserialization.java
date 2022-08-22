package com.whylogs.core.metrics.deserializers;

import whylogs.core.message.MetricComponentMessage;

import java.util.function.Function;

public interface IDeserialization<T> extends Function<MetricComponentMessage, T> {
    public T deserialize(MetricComponentMessage message);
}
