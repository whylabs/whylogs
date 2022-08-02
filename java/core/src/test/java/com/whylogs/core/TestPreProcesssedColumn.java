package com.whylogs.core;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test
public class TestPreProcesssedColumn {

    private void assert_zero_len(List<Object> values){
        if(values == null){
            return;
        }
        Assert.assertEquals(values.size(), 0);
    }

    @Test
    public void test_floats_ints_strings(){
        ArrayList<Object> mixed = new ArrayList<>();
        PreProcessedColumn results;

        mixed.add(1.0);
        mixed.add(2.0);
        mixed.add(1);
        mixed.add(2);
        mixed.add("hello");
        mixed.add(null);

        results = PreProcessedColumn.apply(mixed);

        Assert.assertEquals(results.getLists().getDoubles(), mixed.subList(0, 2));
        Assert.assertEquals(results.getLists().getInts(), mixed.subList(2, 4));
        Assert.assertEquals(results.getLists().getStrings(), mixed.subList(4, 5));
        assert_zero_len(results.getLists().getObjects());
        Assert.assertEquals(results.getNullCount(), 1);

    }

}
