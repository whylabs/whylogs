package com.whylogs.core.views;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestSummaryType {

    @Test
    public void testSummaryType() {
        Assert.assertEquals(SummaryType.COLUMN.label, "COLUMN");
        Assert.assertEquals(SummaryType.DATASET.label, "DATASET");
    }
}
