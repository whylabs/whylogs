package com.whylogs.core.metrics;

import com.whylogs.core.PreProcessedColumn;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;

@Test
public class TestIntegralMetric {

    @Test
    public void test_columnarUpdate_emptyData() {
        IntegralMetric metrics = new IntegralMetric();
        ArrayList<Integer> testList = new ArrayList<>();
        OperationResult result = metrics.columnarUpdate(PreProcessedColumn.apply(testList));
        Assert.assertEquals(result, OperationResult.ok());

        // make sure these are zerod out
        Assert.assertEquals((int) metrics.getMaxComponent().getValue(), Integer.MIN_VALUE);
        Assert.assertEquals((int) metrics.getMinComponent().getValue(), Integer.MAX_VALUE);
    }

    @Test
    public void test_columnarUpdate_unhandledData(){
        IntegralMetric metrics = new IntegralMetric();
        ArrayList<String> testList = new ArrayList<>();
        OperationResult result;

        testList.add("hello");
        testList.add("world");

        result = metrics.columnarUpdate(PreProcessedColumn.apply(testList));
        Assert.assertEquals(result, OperationResult.ok(0));

        // make sure these are zeroed out
        Assert.assertEquals((int) metrics.getMaxComponent().getValue(), Integer.MIN_VALUE);
        Assert.assertEquals((int) metrics.getMinComponent().getValue(), Integer.MAX_VALUE);
    }

    @Test
    public void test_columnarUpdate_integerData(){
        IntegralMetric metrics = new IntegralMetric();
        ArrayList<Integer> testList = new ArrayList<>();
        OperationResult result;

        testList.add(1);
        testList.add(2);
        testList.add(3);

        result = metrics.columnarUpdate(PreProcessedColumn.apply(testList));
        Assert.assertEquals(result, OperationResult.ok(testList.size()));

        // make sure these are zeroed out
        Assert.assertEquals((int) metrics.getMaxComponent().getValue(), 3);
        Assert.assertEquals((int) metrics.getMinComponent().getValue(), 1);
    }

}
