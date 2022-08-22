package com.whylogs.core.metrics.deserializer;

import com.whylogs.core.metrics.deserializers.DeserializerRegistry;
import com.whylogs.core.metrics.deserializers.FloatDeserializer;
import com.whylogs.core.metrics.deserializers.IntDeserializer;
import org.testng.Assert;
import org.testng.annotations.Test;
import whylogs.core.message.MetricComponentMessage;

@Test
public class TestDeserializer {

  @Test
  public void test_deserializationRegistry() {
    DeserializerRegistry registry = DeserializerRegistry.getInstance();
    Assert.assertNotNull(registry);
    Assert.assertEquals(registry, DeserializerRegistry.getInstance());

    IntDeserializer deserializer = new IntDeserializer();
    registry.register(1, deserializer);
    Assert.assertEquals(registry.get(1), deserializer);

    registry.register("test", deserializer);
    Assert.assertEquals(registry.get("test"), deserializer);

    MetricComponentMessage message =
        MetricComponentMessage.newBuilder().setTypeId(1).setN(20).build();
    Assert.assertEquals(registry.get(1).deserialize(message), (long) 20);
  }

  @Test
  public void test_intDeserialization() {
    MetricComponentMessage message =
        MetricComponentMessage.newBuilder().setTypeId(1).setN(20).build();
    IntDeserializer deserializer = new IntDeserializer();
    long value = deserializer.deserialize(message);
    Assert.assertEquals(value, 20);
  }

  @Test
  public void test_floatDeserialization() {
    MetricComponentMessage f_message =
        MetricComponentMessage.newBuilder().setTypeId(2).setD(20.0).build();
    FloatDeserializer f_deserializer = new FloatDeserializer();
    double f_value = f_deserializer.deserialize(f_message);
    Assert.assertEquals(f_value, 20.0);
  }
}
