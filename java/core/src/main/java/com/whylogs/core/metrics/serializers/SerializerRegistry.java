package com.whylogs.core.metrics.serializers;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.whylogs.core.message.MetricComponentMessage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

public class SerializerRegistry {
  private final HashMap<String, ISerialization> namedSerializer;
  private final HashMap<Integer, ISerialization> idSerializer;

  private SerializerRegistry() {
    this.namedSerializer = new HashMap<>();
    this.idSerializer = new HashMap<>();
  }

  // Instance holder to avoid double-checking antipattern for singleton
  private static final class InstanceHolder {
    static final SerializerRegistry instance = new SerializerRegistry();
  }

  public static SerializerRegistry getInstance() {
    return InstanceHolder.instance;
  }

  public <T, A extends ISerialization> void register(String name, A serializer) {
    namedSerializer.put(name, serializer);
  }

  public <T, A extends ISerialization> void register(int typeId, A serializer) {
    idSerializer.put(typeId, serializer);
  }

  public ISerialization get(int typeId) {
    return idSerializer.get(typeId);
  }

  public ISerialization get(String typeName) {
    return namedSerializer.get(typeName);
  }

  public static MetricComponentMessage invokeMethod(Method m, Object parameter) {
    try {
      return (MetricComponentMessage) m.invoke(null, parameter);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new UncheckedExecutionException(e);
    }
  }
}
