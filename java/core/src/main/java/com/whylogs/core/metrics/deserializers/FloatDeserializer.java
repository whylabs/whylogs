package com.whylogs.core.metrics.deserializers;

import whylogs.core.message.MetricComponentMessage;

public class FloatDeserializer implements IDeserialization<Double> {

  @Override
  public Double deserialize(MetricComponentMessage message) {
    return message.getD();
  }

  @Override
  public Double apply(MetricComponentMessage message) {
    return this.deserialize(message);
  }
}
