package com.whylogs.core;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestDataTypes {

    @Test
    public void test_enum_datatypes() {
        DataTypes dataTypes = DataTypes.Integral;
        Assert.assertEquals(dataTypes.name(), "Integral");
        Assert.assertTrue(dataTypes.includes(Integer.class));
        Assert.assertFalse(dataTypes.includes(String.class));
    }
}
