package com.whylogs.core.metrics.components;

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
}
