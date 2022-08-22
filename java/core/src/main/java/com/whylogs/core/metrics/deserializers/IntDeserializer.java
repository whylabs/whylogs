package com.whylogs.core.metrics.deserializers;

import whylogs.core.message.MetricComponentMessage;

public class IntDeserializer implements IDeserialization<Long> {
    @Override
    public Long deserialize(MetricComponentMessage message) {
        return message.getN();
    }

    @Override
    public Long apply(MetricComponentMessage message) {
        return this.deserialize(message);
    }
}
