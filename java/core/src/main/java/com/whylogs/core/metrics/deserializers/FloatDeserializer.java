package com.whylogs.core.metrics.deserializers;

import com.whylogs.core.message.MetricComponentMessage;

public class FloatDeserializer implements Deserializable<Double> {

  @Override
  public Double deserialize(MetricComponentMessage message) {
    return message.getD();
  }

  @Override
  public Double apply(MetricComponentMessage message) {
    return this.deserialize(message);
  }
}
