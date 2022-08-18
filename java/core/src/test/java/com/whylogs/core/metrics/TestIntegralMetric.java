package com.whylogs.core.metrics;

import com.whylogs.core.PreprocessedColumn;
import com.whylogs.core.SummaryConfig;
import java.util.ArrayList;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test
public class TestIntegralMetric {

  @Test
  public void test_zeroAndSummaryDict() {
    IntegralMetric metric = IntegralMetric.zero(new MetricConfig());

    Assert.assertEquals((int) metric.getMaxComponent().getValue(), Integer.MIN_VALUE);
    Assert.assertEquals((int) metric.getMinComponent().getValue(), Integer.MAX_VALUE);

    HashMap<String, Object> summary = metric.toSummaryDict(new SummaryConfig());
    Assert.assertEquals(summary.get("max"), Integer.MIN_VALUE);
    Assert.assertEquals(summary.get("min"), Integer.MAX_VALUE);
  }

  @Test
  public void test_namespace() {
    IntegralMetric metric = new IntegralMetric();
    Assert.assertEquals(metric.getNamespace(), IntegralMetric.NAMESPACE);
  }

  @Test
  public void test_columnarUpdate_emptyData() {
    IntegralMetric metrics = new IntegralMetric();
    ArrayList<Integer> testList = new ArrayList<>();
    OperationResult result = metrics.columnarUpdate(PreprocessedColumn.apply(testList));
    Assert.assertEquals(result, OperationResult.ok());

    // make sure these are zerod out
    Assert.assertEquals((int) metrics.getMaxComponent().getValue(), Integer.MIN_VALUE);
    Assert.assertEquals((int) metrics.getMinComponent().getValue(), Integer.MAX_VALUE);
  }

  @Test
  public void test_columnarUpdate_unhandledData() {
    IntegralMetric metrics = new IntegralMetric();
    ArrayList<String> testList = new ArrayList<>();
    OperationResult result;

    testList.add("hello");
    testList.add("world");

    result = metrics.columnarUpdate(PreprocessedColumn.apply(testList));
    Assert.assertEquals(result, OperationResult.ok(0));

    // make sure these are zeroed out
    Assert.assertEquals((int) metrics.getMaxComponent().getValue(), Integer.MIN_VALUE);
    Assert.assertEquals((int) metrics.getMinComponent().getValue(), Integer.MAX_VALUE);
  }

  @Test
  public void test_columnarUpdate_integerData() {
    IntegralMetric metrics = new IntegralMetric();
    ArrayList<Integer> testList = new ArrayList<>();
    OperationResult result;

    testList.add(1);
    testList.add(2);
    testList.add(3);

    result = metrics.columnarUpdate(PreprocessedColumn.apply(testList));
    Assert.assertEquals(result, OperationResult.ok(testList.size()));

    // make sure these are zeroed out
    Assert.assertEquals((int) metrics.getMaxComponent().getValue(), 3);
    Assert.assertEquals((int) metrics.getMinComponent().getValue(), 1);
  }

  @DataProvider(name = "merge_data")
  public Object[][] merge_data() {
    ArrayList<Integer> testList = new ArrayList<>();
    OperationResult result;

    testList.add(1);
    testList.add(2);
    testList.add(3);

    // Second list
    ArrayList<Integer> testList2 = new ArrayList<>();
    testList2.add(4);
    testList2.add(5);
    testList2.add(6);

    // Second list
    ArrayList<Integer> with_negative = new ArrayList<>();
    with_negative.add(-2);
    with_negative.add(5);
    with_negative.add(6);

    return new Object[][] {
      {testList, testList2, 6, 1},
      {testList2, testList, 6, 1},
      {testList, with_negative, 6, -2},
      {with_negative, testList2, 6, -2},
      {with_negative, with_negative, 6, -2}
    };
  }

  @Test(dataProvider = "merge_data")
  public void test_merge(
      ArrayList<Integer> a, ArrayList<Integer> b, int expectedMax, int expectedMin) {
    IntegralMetric metrics = new IntegralMetric();
    metrics.columnarUpdate(PreprocessedColumn.apply(a));

    IntegralMetric metrics2 = new IntegralMetric();
    metrics2.columnarUpdate(PreprocessedColumn.apply(b));

    IntegralMetric merged = metrics.merge(metrics2);

    // make sure these are zeroed out
    Assert.assertEquals((int) merged.getMaxComponent().getValue(), expectedMax);
    Assert.assertEquals((int) merged.getMinComponent().getValue(), expectedMin);
  }
}
