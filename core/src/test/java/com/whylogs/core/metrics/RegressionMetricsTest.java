package com.whylogs.core.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import lombok.val;
import org.testng.annotations.Test;

public class RegressionMetricsTest {
  @Test
  public void basic_regression_test() {
    val metrics = new RegressionMetrics("prediction", "target");
    metrics.track(ImmutableMap.of("prediction", 1, "target", 1));
    metrics.track(ImmutableMap.of("prediction", 2, "target", 2));
    metrics.track(ImmutableMap.of("prediction", 3, "target", 3));

    assertThat(metrics.getSumDiff(), is(0.0));
    assertThat(metrics.getSum2Diff(), is(0.0));
    assertThat(metrics.getSumAbsDiff(), is(0.0));
    assertThat(metrics.getCount(), is(3L));
  }

  @Test
  public void basic_merge_null() {
    val metrics = new RegressionMetrics("prediction", "target");
    metrics.track(ImmutableMap.of("prediction", 1, "target", 1));
    metrics.track(ImmutableMap.of("prediction", 2, "target", 2));
    metrics.track(ImmutableMap.of("prediction", 3, "target", 3));

    val merged = metrics.merge(null);
    assertThat(merged.getSumDiff(), is(0.0));
    assertThat(merged.getSum2Diff(), is(0.0));
    assertThat(merged.getSumAbsDiff(), is(0.0));
  }

  @Test
  public void basic_merge_empty() {
    val metrics = new RegressionMetrics("prediction", "target");
    metrics.track(ImmutableMap.of("prediction", 1, "target", 1));
    metrics.track(ImmutableMap.of("prediction", 2, "target", 2));
    metrics.track(ImmutableMap.of("prediction", 3, "target", 3));

    val merged = metrics.merge(new RegressionMetrics("prediction", "target"));
    assertThat(merged.getSumDiff(), is(0.0));
    assertThat(merged.getSum2Diff(), is(0.0));
    assertThat(merged.getSumAbsDiff(), is(0.0));
    assertThat(metrics.getCount(), is(3L));
  }
}
