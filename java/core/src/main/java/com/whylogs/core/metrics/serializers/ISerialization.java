package com.whylogs.core.metrics.serializers;

import java.util.function.Function;
import whylogs.core.message.MetricComponentMessage;

public interface ISerialization<T> extends Function<T, MetricComponentMessage> {
  public MetricComponentMessage serialize(T value);
}
