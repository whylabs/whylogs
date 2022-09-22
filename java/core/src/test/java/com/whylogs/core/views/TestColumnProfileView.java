package com.whylogs.core.views;

import com.whylogs.core.SummaryConfig;
import com.whylogs.core.errors.UnsupportedError;
import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.metrics.components.MetricComponent;
import java.util.*;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestColumnProfileView {

  private ColumnProfileView getDefaultColumnProfile() {
    Metric integralMetric = IntegralMetric.zero(new MetricConfig());
    HashMap<String, Metric> metrics = new HashMap<>();
    metrics.put(integralMetric.getNamespace(), integralMetric);

    return new ColumnProfileView(metrics);
  }

  private ColumnProfileView getChangedSuccessFailProfile(int success, int fail) {
    Metric integralMetric = IntegralMetric.zero(new MetricConfig());
    HashMap<String, Metric> metrics = new HashMap<>();
    metrics.put(integralMetric.getNamespace(), integralMetric);

    return new ColumnProfileView(metrics, success, fail);
  }

  @Test
  public void testColumnProfileInit() {
    ColumnProfileView columnProfileView = getDefaultColumnProfile();

    Assert.assertEquals(columnProfileView.getMetric("ints").get().getClass(), IntegralMetric.class);
    Assert.assertEquals(columnProfileView.getFailures(), 0);
    Assert.assertEquals(columnProfileView.getSuccesses(), 0);

    columnProfileView = getChangedSuccessFailProfile(1, 2);
    Assert.assertEquals(columnProfileView.getFailures(), 2);
    Assert.assertEquals(columnProfileView.getSuccesses(), 1);

    columnProfileView = ColumnProfileView.zero();
    Assert.assertEquals(columnProfileView.getFailures(), 0);
    Assert.assertEquals(columnProfileView.getSuccesses(), 0);
  }

  @Test
  public void testMerge() {
    ColumnProfileView columnProfileView = getDefaultColumnProfile();
    ColumnProfileView columnProfileView2 = getChangedSuccessFailProfile(1, 2);

    ColumnProfileView result = columnProfileView.merge(columnProfileView2);

    Assert.assertEquals(result.getMetric("ints").get().getClass(), IntegralMetric.class);
    Assert.assertEquals(result.getFailures(), 2);
    Assert.assertEquals(result.getSuccesses(), 1);
  }

  @Test
  public void testMergeWithNull() {
    ColumnProfileView columnProfileView = getDefaultColumnProfile();
    ColumnProfileView result = columnProfileView.merge(null);

    Assert.assertEquals(result.getMetric("ints").get().getClass(), IntegralMetric.class);
    Assert.assertEquals(result.getFailures(), 0);
    Assert.assertEquals(result.getSuccesses(), 0);
  }

  @Test
  public void testGetMetricComponentPaths() {
    ColumnProfileView columnProfileView = getDefaultColumnProfile();
    ArrayList<String> paths = columnProfileView.getMetricComponentPaths();
    Assert.assertEquals(paths.size(), 2);
    Assert.assertEquals(paths.get(0), "ints/MaxIntegralComponent");
    Assert.assertEquals(paths.get(1), "ints/MinIntegralComponent");
  }

  @Test
  public void testGetMetricComponentPathsEmpty() {
    ColumnProfileView columnProfileView = ColumnProfileView.zero();
    ArrayList<String> paths = columnProfileView.getMetricComponentPaths();
    Assert.assertEquals(paths.size(), 0);
  }

  @Test
  public void testGetMetricComponentPathsNull() {
    ColumnProfileView columnProfileView = getDefaultColumnProfile();
    columnProfileView = columnProfileView.merge(null);
    ArrayList<String> paths = columnProfileView.getMetricComponentPaths();
    Assert.assertEquals(paths.size(), 2);
    Assert.assertEquals(paths.get(0), "ints/MaxIntegralComponent");
    Assert.assertEquals(paths.get(1), "ints/MinIntegralComponent");
  }

  @Test
  public void testToSummaryDict() throws UnsupportedError {
    ColumnProfileView columnProfileView = getDefaultColumnProfile();
    HashMap<String, Object> summary =
        columnProfileView.toSummaryDict(
            Optional.ofNullable("ints"), Optional.ofNullable(new SummaryConfig()));
    Assert.assertEquals(summary.size(), 2);
    Assert.assertEquals(summary.get("ints/min"), Integer.MAX_VALUE);
    Assert.assertEquals(summary.get("ints/max"), Integer.MIN_VALUE);

    summary = columnProfileView.toSummaryDict(Optional.empty(), Optional.empty());
    Assert.assertEquals(summary.size(), 2);
    Assert.assertEquals(summary.get("ints/min"), Integer.MAX_VALUE);
    Assert.assertEquals(summary.get("ints/max"), Integer.MIN_VALUE);
  }

  @Test
  public void testGetComponents() {
    ColumnProfileView columnProfileView = getDefaultColumnProfile();
    Map<String, MetricComponent> components = columnProfileView.getComponents();
    Assert.assertEquals(components.size(), 2);
    Assert.assertEquals(components.get("MinIntegralComponent").getValue(), Integer.MAX_VALUE);
    Assert.assertEquals(components.get("MaxIntegralComponent").getValue(), Integer.MIN_VALUE);
  }
}
