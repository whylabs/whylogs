package com.whylogs.core.metrics.components;

import com.whylogs.core.message.MetricComponentMessage;
import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class TestMinMaxComponents {

  public static final int MIN_TYPE = 1;
  public static final int MAX_TYPE = 2;

  private ArrayList<Integer> with_negative() {
    ArrayList<Integer> with_negative = new ArrayList<>();
    with_negative.add(-1);
    with_negative.add(4);
    with_negative.add(-5);
    with_negative.add(6);
    with_negative.add(0);
    return with_negative;
  }

  private ArrayList<Integer> non_negative() {
    ArrayList<Integer> only_positive = new ArrayList<>();
    only_positive.add(0);
    only_positive.add(20);
    return only_positive;
  }

  private ArrayList<Integer> singleton() {
    ArrayList<Integer> singleton = new ArrayList<>();
    singleton.add(1);
    return singleton;
  }

  @DataProvider(name = "min-data-provider")
  public Object[][] minDataProvider() {
    return new Object[][] {
      {with_negative(), -5},
      {non_negative(), 0},
      {singleton(), 1},
      {null, Integer.MAX_VALUE}
    };
  }

  @DataProvider(name = "max-data-provider")
  public Object[][] maxDataProvider() {
    return new Object[][] {
      {with_negative(), 6},
      {non_negative(), 20},
      {singleton(), 1},
      {null, Integer.MIN_VALUE}
    };
  }

  @Test(dataProvider = "min-data-provider")
  public void testMin(ArrayList<Integer> input, int expected) {
    MinIntegralComponent min;

    if (input == null) {
      min = new MinIntegralComponent();
    } else {
      // Test method of two ints method
      if (input.size() == 2) {
        min = MinIntegralComponent.min(input.get(0), input.get(1));
        Assert.assertEquals((int) min.getValue(), expected);
      }

      // this always tests the array
      min = MinIntegralComponent.min(input);
    }

    Assert.assertEquals((int) min.getValue(), expected);
    Assert.assertEquals(min.getTypeId(), MIN_TYPE);

    MinIntegralComponent min2 = min.copy();
    Assert.assertEquals(min2.getTypeId(), MIN_TYPE);
    Assert.assertEquals((int) min2.getValue(), expected);
  }

  @Test(dataProvider = "max-data-provider")
  public void testMax(ArrayList<Integer> input, int expected) {
    MaxIntegralComponent max;

    if (input == null) {
      max = new MaxIntegralComponent();
    } else {
      // Test method of two ints method
      if (input.size() == 2) {
        max = MaxIntegralComponent.max(input.get(0), input.get(1));
        Assert.assertEquals((int) max.getValue(), expected);
      }

      // this always tests the array method
      max = MaxIntegralComponent.max(input);
    }

    Assert.assertEquals((int) max.getValue(), expected);
    Assert.assertEquals(max.getTypeId(), MAX_TYPE);

    MaxIntegralComponent max2 = max.copy();
    Assert.assertEquals(max2.getTypeId(), MAX_TYPE);
    Assert.assertEquals((int) max2.getValue(), expected);
  }

  @Test
  public void testToProtobuf() {
    MaxIntegralComponent component = new MaxIntegralComponent(10);
    MetricComponentMessage message = component.toProtobuf();
    Assert.assertEquals(message.getTypeId(), 2);
    Assert.assertEquals(message.getN(), 10);

    MinIntegralComponent min_com = new MinIntegralComponent(-10);
    MetricComponentMessage min_message = min_com.toProtobuf();
    Assert.assertEquals(min_message.getTypeId(), 1);
    Assert.assertEquals(min_message.getN(), -10);
  }

  @Test
  public void testFromProtobuf() {
    MetricComponentMessage max_message =
        MetricComponentMessage.newBuilder().setTypeId(2).setN(20).build();
    MaxIntegralComponent max_component =
        (MaxIntegralComponent) MetricComponent.fromProtobuf(max_message);
    Assert.assertEquals((int) max_component.getValue(), 20);

    MetricComponentMessage min_message =
        MetricComponentMessage.newBuilder().setTypeId(1).setN(-10).build();
    MinIntegralComponent min_component =
        (MinIntegralComponent) MetricComponent.fromProtobuf(min_message);
    Assert.assertEquals((int) min_component.getValue(), -10);
  }
}
