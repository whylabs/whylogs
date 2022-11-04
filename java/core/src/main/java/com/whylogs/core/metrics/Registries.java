package com.whylogs.core.metrics;

import com.whylogs.core.metrics.aggregators.AggregatorRegistry;
import com.whylogs.core.metrics.components.MetricComponent;
import com.whylogs.core.metrics.deserializers.DeserializerRegistry;
import com.whylogs.core.metrics.serializers.SerializerRegistry;
import lombok.Data;

@Data
public class Registries {
    private AggregatorRegistry aggregatorRegistry;
    private SerializerRegistry serializerRegistry;
    private DeserializerRegistry deserializerRegistry;

    private static Registries instance;

    private Registries() {
        aggregatorRegistry = AggregatorRegistry.getInstance();
        serializerRegistry = SerializerRegistry.getInstance();
        deserializerRegistry = DeserializerRegistry.getInstance();
    }

    public static Registries getInstance() {
        if (instance == null) {
            instance = new Registries();
        }
        return instance;
    }

    public static void reset() {
        instance = null;
    }

}
