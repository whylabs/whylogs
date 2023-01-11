package com.whylogs.core.metrics.deserializers;

import com.whylogs.core.message.MetricComponentMessage;

public class IntDeserializer implements Deserializable<Integer> {
  @Override
  public Integer deserialize(MetricComponentMessage message) {
    return (int) message.getN();
  }

  @Override
  public Integer apply(MetricComponentMessage message) {
    return this.deserialize(message);
  }
}
