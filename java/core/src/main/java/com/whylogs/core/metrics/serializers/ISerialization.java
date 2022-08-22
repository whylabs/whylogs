package com.whylogs.core.metrics.serializers;

import com.whylogs.core.message.MetricComponentMessage;
import java.util.function.Function;

public interface ISerialization<T> extends Function<T, MetricComponentMessage> {
  MetricComponentMessage serialize(T value);
}
