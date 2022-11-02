package com.whylogs.core.metrics.deserializers;

import com.whylogs.core.message.MetricComponentMessage;
import java.util.function.Function;

public interface Deserializable<T> extends Function<MetricComponentMessage, T> {
  T deserialize(MetricComponentMessage message);
}
