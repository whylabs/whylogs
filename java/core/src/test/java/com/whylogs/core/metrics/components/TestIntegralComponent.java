package com.whylogs.core.metrics.components;

import com.whylogs.core.message.MetricComponentMessage;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestIntegralComponent {

  @Test
  public void testIntegral() {
    IntegralComponent component = new IntegralComponent(1);
    Assert.assertEquals((int) component.getValue(), 1);

    component = new IntegralComponent();
    Assert.assertEquals((int) component.getValue(), 0);

    Assert.assertEquals(component.getTypeId(), 0);
  }

  @Test
  public void testCopy() {
    IntegralComponent component = new IntegralComponent(1);
    Assert.assertEquals((int) component.getValue(), 1);
    Assert.assertEquals((int) component.copy().getValue(), 1);
  }

  @Test
  public void testToProtobuf() {
    IntegralComponent component = new IntegralComponent(10);
    MetricComponentMessage message = component.toProtobuf();
    Assert.assertEquals(message.getTypeId(), 0);
    Assert.assertEquals(message.getN(), 10);
  }

  @Test
  public void testFromProtobuf() {
    MetricComponentMessage message = MetricComponentMessage.newBuilder().setTypeId(0).setN(10).build();
    IntegralComponent component = IntegralComponent.fromProtobuf(message);
    Assert.assertEquals((int) component.getValue(), 10);
  }
}
