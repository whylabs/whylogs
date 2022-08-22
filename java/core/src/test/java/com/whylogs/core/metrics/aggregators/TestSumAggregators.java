package com.whylogs.core.metrics.aggregators;

import com.whylogs.core.errors.UnsupportedError;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class TestSumAggregators {

  @DataProvider(name = "intData")
  public Object[][] intData() {
    return new Object[][] {
      {1, 2, 3},
      {-1, -2, -3},
      {Integer.MIN_VALUE, Integer.MAX_VALUE, -1},
      {-3, 2, -1}
    };
  }

  @Test(dataProvider = "intData")
  public void testIntegerSum(int lhs, int rhs, int expected) throws UnsupportedError {
    NumberSumAggregator<Integer> aggregator = new NumberSumAggregator<>();
    int result = 0;

    try {
      result = aggregator.merge(lhs, rhs);
    } catch (UnsupportedError e) {
      Assert.fail();
    }

    Assert.assertEquals(result, expected);
  }

  @DataProvider(name = "doubleData")
  public Object[][] doubleData() {
    return new Object[][] {
      {1.0, 2.5, 3.5},
      {-1.0, -2.0, -3.0},
      {-3.0, 2.0, -1.0}
    };
  }

  @Test(dataProvider = "doubleData")
  public void testDoubleSum(Double lhs, Double rhs, Double expected) throws UnsupportedError {
    NumberSumAggregator<Double> aggregator = new NumberSumAggregator<>();
    double result = 0;

    try {
      result = aggregator.merge(lhs, rhs);
    } catch (UnsupportedError e) {
      Assert.fail();
    }

    Assert.assertEquals(result, expected);
  }

  @DataProvider(name = "floatData")
  public Object[][] floatData() {
    float a = 1;
    float b = 2;
    float c = 3;
    float d = -1;
    return new Object[][] {
      {a, b, c},
      {d, c, b},
    };
  }

  @Test(dataProvider = "floatData")
  public void testFloatSum(Float lhs, Float rhs, Float expected) throws UnsupportedError {
    NumberSumAggregator<Float> aggregator = new NumberSumAggregator<>();
    float result = 0;

    try {
      result = aggregator.merge(lhs, rhs);
    } catch (UnsupportedError e) {
      Assert.fail();
    }

    Assert.assertEquals(result, expected);
  }
}
