package com.whylogs.core.metrics.serializers;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.whylogs.core.message.MetricComponentMessage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

public class SerializerRegistry {
  private final HashMap<String, Serializable> namedSerializer;
  private final HashMap<Integer, Serializable> idSerializer;

  private SerializerRegistry() {
    this.namedSerializer = new HashMap<>();
    this.idSerializer = new HashMap<>();

    // TODO: singleton the serializers? No need to make seperate instances
    // Register Defaults
    this.namedSerializer.put("int", new NumberSerializer());
    this.namedSerializer.put("float", new NumberSerializer());

    this.idSerializer.put(0, new NumberSerializer());
    this.idSerializer.put(1, new NumberSerializer());
    this.idSerializer.put(2, new NumberSerializer());
  }

  // Instance holder to avoid double-checking antipattern for singleton
  private static final class InstanceHolder {
    static final SerializerRegistry instance = new SerializerRegistry();
  }

  public static SerializerRegistry getInstance() {
    return InstanceHolder.instance;
  }

  public <T, A extends Serializable> void register(String name, A serializer) {
    namedSerializer.put(name, serializer);
  }

  public <T, A extends Serializable> void register(int typeId, A serializer) {
    idSerializer.put(typeId, serializer);
  }

  public Serializable get(int typeId) {
    return idSerializer.get(typeId);
  }

  public Serializable get(String typeName) {
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
