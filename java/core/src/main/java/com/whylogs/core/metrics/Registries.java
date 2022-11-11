package com.whylogs.core.metrics;

import com.whylogs.core.metrics.aggregators.AggregatorRegistry;
import com.whylogs.core.metrics.components.ComponentRegistry;
import com.whylogs.core.metrics.deserializers.DeserializerRegistry;
import com.whylogs.core.metrics.serializers.SerializerRegistry;
import lombok.Data;

@Data
public class Registries {
  private AggregatorRegistry aggregatorRegistry;
  private SerializerRegistry serializerRegistry;
  private DeserializerRegistry deserializerRegistry;
  private ComponentRegistry componentRegistry;

  // Question: should registires be a singleton as well?
  private static Registries instance;

  private Registries() {
    aggregatorRegistry = AggregatorRegistry.getInstance();
    serializerRegistry = SerializerRegistry.getInstance();
    deserializerRegistry = DeserializerRegistry.getInstance();
    componentRegistry = ComponentRegistry.getInstance();
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
