package com.whylogs.core.metrics.deserializers;

import java.util.HashMap;

public class DeserializerRegistry {
  private final HashMap<String, Deserializable> namedSerializer;
  private final HashMap<Integer, Deserializable> idSerializer;

  private DeserializerRegistry() {
    this.namedSerializer = new HashMap<>();
    this.idSerializer = new HashMap<>();
  }

  // Instance holder to avoid double-checking antipattern for singleton
  private static final class InstanceHolder {
    static final DeserializerRegistry instance = new DeserializerRegistry();
  }

  public static DeserializerRegistry getInstance() {
    return InstanceHolder.instance;
  }

  public <T, A extends Deserializable> void register(String name, A deserializer) {
    namedSerializer.put(name, deserializer);
  }

  public <T, A extends IntDeserializer> void register(int typeId, A deserializer) {
    idSerializer.put(typeId, deserializer);
  }

  public Deserializable get(int typeId) {
    return idSerializer.get(typeId);
  }

  public Deserializable get(String typeName) {
    return namedSerializer.get(typeName);
  }
}
