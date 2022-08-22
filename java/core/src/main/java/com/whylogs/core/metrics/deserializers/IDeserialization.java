package com.whylogs.core.metrics.deserializers;

import java.util.function.Function;
import whylogs.core.message.MetricComponentMessage;

public interface IDeserialization<T> extends Function<MetricComponentMessage, T> {
  public T deserialize(MetricComponentMessage message);
}
