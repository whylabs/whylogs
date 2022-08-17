package com.whylogs.core.metrics.aggregators;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.function.BinaryOperator;

@Test
public class TestAggregatorRegistry {

    @Test
    public void test_registry_singleton(){
        AggregatorRegistry registry = AggregatorRegistry.getInstance();
        AggregatorRegistry registry2 = AggregatorRegistry.getInstance();
        Assert.assertEquals(registry, registry2);
    }

    @Test
    public void test_registry_add(){
        // try with a binary operator, we need the typing so you can't seem to do an
        // inline lambda
        AggregatorRegistry registry = AggregatorRegistry.getInstance();
        BinaryOperator<Integer> aggregator = (lhs, rhs) -> lhs + rhs;
        registry.register(1,  aggregator);
        Assert.assertEquals(registry.get(1), aggregator);

        registry.register(2,  new NumberSumAggregator<Integer>());
        int sum = (int) registry.get(2).apply(1, 2);
        Assert.assertEquals(sum, 3);
    }
}
