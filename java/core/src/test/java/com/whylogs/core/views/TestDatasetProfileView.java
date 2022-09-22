package com.whylogs.core.views;

import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.HashMap;

public class TestDatasetProfileView {

    private DatasetProfileView getDefaultDatasetProfile(){
        HashMap<String, ColumnProfileView> columnProfileViews = new HashMap<>();
        HashMap<String, Metric> testMetrics = new HashMap<>();
        testMetrics.put("ints", IntegralMetric.zero(new MetricConfig()));
        columnProfileViews.put("test", new ColumnProfileView(testMetrics));
        return new DatasetProfileView(columnProfileViews, new Date(), new Date());
    }

    @Test
    public void testDatasetProfileViewInit(){
        DatasetProfileView view = new DatasetProfileView(new HashMap<String, ColumnProfileView>(), new Date(), new Date());
        Assert.assertEquals(view.getColumns().size(), 0);

        view = getDefaultDatasetProfile();
        Assert.assertEquals(view.getColumns().size(), 1);
        Assert.assertNotNull(view.getColumns().get("test").getMetric("ints"));
    }

    @Test
    public void testMerge(){
        DatasetProfileView view = getDefaultDatasetProfile();
        DatasetProfileView view2 = getDefaultDatasetProfile();
        DatasetProfileView result = view.merge(view2);
        Assert.assertEquals(result.getColumns().size(), 1);
        Assert.assertNotNull(result.getColumns().get("test").getMetric("ints"));
    }

    @Test
    public void testMergeWithNull(){
        DatasetProfileView view = getDefaultDatasetProfile();
        DatasetProfileView result = view.merge(null);
        Assert.assertEquals(result.getColumns().size(), 1);
        Assert.assertNotNull(result.getColumns().get("test").getMetric("ints"));
    }

    @Test
    public void testMergeWithEmpty(){
        DatasetProfileView view = getDefaultDatasetProfile();
        DatasetProfileView result = view.merge(new DatasetProfileView(new HashMap<String, ColumnProfileView>(), new Date(), new Date()));
        Assert.assertEquals(result.getColumns().size(), 1);
        Assert.assertNotNull(result.getColumns().get("test").getMetric("ints"));
    }

    @Test
    public void testGetColumn(){
        DatasetProfileView view = getDefaultDatasetProfile();
        Assert.assertNotNull(view.getColumn("test"));
    }

    @Test
    public void testGetColumns(){
        DatasetProfileView view = getDefaultDatasetProfile();
        Assert.assertNotNull(view.getColumns());
    }
}
