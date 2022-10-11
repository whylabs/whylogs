package com.whylogs.api.resultsets;

import com.whylogs.api.logger.resultSets.ViewResultSet;
import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.views.ColumnProfileView;
import com.whylogs.core.views.DatasetProfileView;
import java.time.Instant;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestViewResultSet {

  private DatasetProfileView getDefaultDatasetProfile(Instant timestamp) {
    HashMap<String, ColumnProfileView> columnProfileViews = new HashMap<>();
    HashMap<String, Metric<?>> testMetrics = new HashMap<>();
    testMetrics.put("ints", IntegralMetric.zero(new MetricConfig()));
    columnProfileViews.put("test", new ColumnProfileView(testMetrics));

    return new DatasetProfileView(columnProfileViews, timestamp, timestamp);
  }

  @Test
  public void testViewResultSet() {
    Instant timestamp = Instant.now();
    DatasetProfileView view = getDefaultDatasetProfile(timestamp);
    ViewResultSet viewResultSet = new ViewResultSet(view);
    Assert.assertNotNull(viewResultSet);

    if (viewResultSet.view().isPresent()) {
      Assert.assertEquals(viewResultSet.view().get().getColumns().size(), 1);
      Assert.assertEquals(
          viewResultSet.view().get().getColumns().get("test").getComponents().size(), 2);
    } else {
      Assert.fail("View is not present");
    }
  }
}
