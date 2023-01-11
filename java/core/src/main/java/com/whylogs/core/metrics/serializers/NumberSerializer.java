package com.whylogs.core.metrics.serializers;

import com.whylogs.core.message.MetricComponentMessage;

// TODO: add annotation support
public class NumberSerializer implements Serializable<Number> {

  @Override
  public MetricComponentMessage.Builder serialize(Number value) {
    MetricComponentMessage.Builder builder = MetricComponentMessage.newBuilder();

    if (value instanceof Long) {
      return builder.setN((long) value);
    }

    if (value instanceof Integer) {
      return builder.setN((int) value);
    }

    if (value instanceof Short) {
      return builder.setN((short) value);
    }

    if (value instanceof Double) {
      return builder.setD((double) value);
    }

    if (value instanceof Float) {
      return builder.setD((float) value);
    }

    throw new IllegalArgumentException("Unsupported number type: " + value.getClass());
  }

  // QUESTION: should this also return a builder so the type can be used?
  // or return a builder and set the type like in serialize? Not sure the API here
  @Override
  public MetricComponentMessage apply(Number value) {
    return this.serialize(value).build();
  }
}
