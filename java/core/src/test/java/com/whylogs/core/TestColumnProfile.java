package com.whylogs.core;

import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.metrics.components.MaxIntegralComponent;
import com.whylogs.core.metrics.components.MinIntegralComponent;
import com.whylogs.core.resolvers.StandardResolver;
import com.whylogs.core.schemas.ColumnSchema;
import com.whylogs.core.views.ColumnProfileView;
import java.util.ArrayList;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestColumnProfile {
  private final String columnName = "testColumn";
  private final int CACHE_SIZE = 2;

  private ColumnProfile<Integer> getDefaultColumnProfile() {
    ColumnSchema standardSchema =
        new ColumnSchema(Integer.class, new MetricConfig(), new StandardResolver());
    ColumnProfile<Integer> result = new ColumnProfile<>(columnName, standardSchema, CACHE_SIZE);
    result.addMetric(IntegralMetric.zero(new MetricConfig()));
    return result;
  }

  @Test
  public void testColumnProfileInit() {
    ColumnProfile<Integer> profile = getDefaultColumnProfile();
    Assert.assertEquals(profile.getName(), columnName);
    Assert.assertEquals(profile.getSchema().getType(), Integer.class);
    Assert.assertEquals(profile.getCacheSize(), CACHE_SIZE);
  }

  @Test
  public void testAddMetric() {
    ColumnProfile<Integer> profile = getDefaultColumnProfile();
    Assert.assertEquals(profile.getMetrics().size(), 1);
    Assert.assertEquals(profile.getMetrics().get("ints").getClass(), IntegralMetric.class);
    IntegralMetric metric = (IntegralMetric) profile.getMetrics().get("ints");
    Assert.assertEquals((int) metric.getMaxComponent().getValue(), Integer.MIN_VALUE);

    IntegralMetric metric2 =
        new IntegralMetric(new MaxIntegralComponent(22), new MinIntegralComponent(20));
    profile.addMetric(metric2);
    Assert.assertEquals(profile.getMetrics().size(), 1);
    IntegralMetric result = (IntegralMetric) profile.getMetrics().get("ints");
    Assert.assertEquals((int) result.getMaxComponent().getValue(), 22);
  }

  @Test
  public void testTrack() {
    ColumnProfile<Integer> profile = getDefaultColumnProfile();
    Assert.assertEquals(profile.getCacheSize(), CACHE_SIZE);

    HashMap<String, Object> row = new HashMap<>();
    row.put(columnName, 5);
    row.put("test2", 2);

    profile.track(row);
    Assert.assertEquals(profile.getCachedValues().size(), 1);
    Assert.assertEquals((int) profile.getCachedValues().get(0), 5);

    row.put(columnName, 2);
    profile.track(row);
    // With cache size of 2 this should have forced a flush
    Assert.assertEquals(profile.getCachedValues().size(), 0);
    Assert.assertEquals(profile.getSuccessCount(), 2);
  }

  @Test
  public void testTrackNull() {
    ColumnProfile<Integer> profile = getDefaultColumnProfile();
    Assert.assertEquals(profile.getCacheSize(), CACHE_SIZE);

    HashMap<String, Object> row = new HashMap<>();
    row.put(columnName, 1);
    profile.track(row);

    row.put(columnName, null);
    profile.track(row);

    Assert.assertEquals(profile.getCachedValues().size(), 0);
    Assert.assertEquals(profile.getSuccessCount(), 1);
    Assert.assertEquals(profile.getFailureCount(), 0);
    // There is a null count in the columnar update, but we don't store it in the profile
  }

  // Because of the typing in Java, how do we trigger the failure?

  @Test
  public void testFlush() {
    ColumnProfile<Integer> profile = getDefaultColumnProfile();

    HashMap<String, Object> row = new HashMap<>();
    row.put(columnName, 5);

    profile.track(row);
    Assert.assertEquals(profile.getCachedValues().size(), 1);

    profile.flush();
    Assert.assertEquals(profile.getCachedValues().size(), 0);
    Assert.assertEquals(profile.getSuccessCount(), 1);
  }

  @Test
  public void testTrackColumn() {
    ColumnProfile<Integer> profile = getDefaultColumnProfile();
    ArrayList<String> column = new ArrayList<>();
    column.add("1");

    profile.trackColumn(column);
    Assert.assertEquals(profile.getSuccessCount(), 0);
    Assert.assertEquals(profile.getFailureCount(), 0);

    ArrayList<Integer> column2 = new ArrayList<>();
    column2.add(1);
    column2.add(2);
    column2.add(null);
    profile.trackColumn(column2);
    Assert.assertEquals(profile.getSuccessCount(), 2);
    Assert.assertEquals(profile.getFailureCount(), 0);
  }

  @Test
  public void testView() {
    ColumnProfile<Integer> profile = getDefaultColumnProfile();
    ArrayList<Integer> column = new ArrayList<>();
    column.add(1);
    column.add(2);
    column.add(null);
    profile.trackColumn(column);

    ColumnProfileView view = profile.view();
    Assert.assertEquals(view.getSuccesses(), 3);
    Assert.assertEquals(view.getFailures(), 0);
    Assert.assertEquals(view.getMetrics().size(), 1);
    Assert.assertEquals(view.getMetrics().get("ints").getClass(), IntegralMetric.class);
    IntegralMetric metric = (IntegralMetric) view.getMetrics().get("ints");
    Assert.assertEquals((int) metric.getMaxComponent().getValue(), 2);
  }
}
