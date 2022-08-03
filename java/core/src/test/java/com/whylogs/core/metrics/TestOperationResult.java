package com.whylogs.core.metrics;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestOperationResult {

    @Test
    public void test_successes(){
        OperationResult result = OperationResult.ok();
        Assert.assertEquals(result.getSuccesses(), 1);

        result = OperationResult.ok(5);
        Assert.assertEquals(result.getSuccesses(), 5);
        Assert.assertEquals(result.getFailures(), 0);
    }

    @Test
    public void test_failures(){
        OperationResult result = OperationResult.failed();
        Assert.assertEquals(result.getFailures(), 1);

        result = OperationResult.failed(5);
        Assert.assertEquals(result.getFailures(), 5);
        Assert.assertEquals(result.getSuccesses(), 0);
    }

    @Test
    public void test_add(){
        OperationResult result = OperationResult.failed();
        result = result.add(OperationResult.ok());
        Assert.assertEquals(result.getSuccesses(), 1);
        Assert.assertEquals(result.getFailures(), 1);
    }

}
