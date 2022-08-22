package com.whylogs.core.metrics.serializers;

import org.testng.Assert;
import org.testng.annotations.Test;
import whylogs.core.message.MetricComponentMessage;

@Test
public class TestSerializer {

    @Test
    public void test_serializationRegistry(){
        SerializerRegistry registry = SerializerRegistry.getInstance();
        Assert.assertNotNull(registry);
        Assert.assertEquals(registry, SerializerRegistry.getInstance());

        NumberSerializer serializer = new NumberSerializer();
        registry.register(1, serializer);
        Assert.assertEquals(registry.get(1), serializer);

        registry.register("test", serializer);
        Assert.assertEquals(registry.get("test"), serializer);
    }

    @Test
    public void test_numberSerialization(){
        NumberSerializer serializer = new NumberSerializer();
        MetricComponentMessage message = serializer.serialize(1);
        Assert.assertEquals(message.getTypeId(), 0);
        Assert.assertEquals(message.getN(), 1);

        serializer = new NumberSerializer();
        message = serializer.serialize(1.0);
        Assert.assertEquals(message.getTypeId(), 0);
        Assert.assertEquals(message.getD(), 1.0);
    }

    @BuiltInSerializer(name="test", typeID=10)
    public MetricComponentMessage testMethod(int value){
        return MetricComponentMessage.newBuilder().setTypeId(10).setN(value).build();
    }

    @Test
    public void test_annotation(){
        SerializerRegistry registry = SerializerRegistry.getInstance();
        Assert.assertEquals(registry.get("test"), registry.get(10));

        MetricComponentMessage testMessage = MetricComponentMessage.newBuilder().setTypeId(10).setN(20).build();
        MetricComponentMessage targetMessage =  registry.get("test").apply(20)
        Assert.assertEquals(targetMessage.getN(), testMessage.getN());
    }
}
