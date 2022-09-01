package com.whylogs.core.metrics.serializers;

import com.whylogs.core.message.MetricComponentMessage;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestSerializer {

  @Test
  public void test_serializationRegistry() {
    SerializerRegistry registry = SerializerRegistry.getInstance();
    Assert.assertNotNull(registry);
    Assert.assertSame(registry, SerializerRegistry.getInstance());

    NumberSerializer serializer = new NumberSerializer();
    registry.register(1, serializer);
    Assert.assertEquals(registry.get(1), serializer);

    registry.register("test", serializer);
    Assert.assertEquals(registry.get("test"), serializer);

    Assert.assertEquals(registry.get(1).serialize(20).getN(), 20);
  }

  @Test
  public void test_numberSerialization() {
    NumberSerializer serializer = new NumberSerializer();
    MetricComponentMessage message = serializer.serialize(1);
    Assert.assertEquals(message.getTypeId(), 0);
    Assert.assertEquals(message.getN(), 1);

    serializer = new NumberSerializer();
    message = serializer.serialize(1.0);
    Assert.assertEquals(message.getTypeId(), 0);
    Assert.assertEquals(message.getD(), 1.0);
  }
}
