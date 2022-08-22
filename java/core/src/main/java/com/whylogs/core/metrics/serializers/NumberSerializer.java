package com.whylogs.core.metrics.serializers;

import whylogs.core.message.MetricComponentMessage;

public class NumberSerializer implements ISerialization<Number> {

    @Override
    public MetricComponentMessage serialize(Number value) {
        if (value instanceof Long) {
            return MetricComponentMessage.newBuilder().setN((long) value).build();
        }

        if (value instanceof Integer) {
            return MetricComponentMessage.newBuilder().setN((int) value).build();
        }

        if (value instanceof Short) {
            return MetricComponentMessage.newBuilder().setN((short) value).build();
        }

        if (value instanceof Double){
            return MetricComponentMessage.newBuilder().setD((double) value).build();
        }

        if (value instanceof Float){
            return MetricComponentMessage.newBuilder().setD((float) value).build();
        }

        throw new IllegalArgumentException("Unsupported number type: " + value.getClass());
    }

    @Override
    public MetricComponentMessage apply(Number value) {
        return this.serialize(value);
    }
}
