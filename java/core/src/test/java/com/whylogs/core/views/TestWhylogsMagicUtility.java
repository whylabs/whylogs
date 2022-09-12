package com.whylogs.core.views;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestWhylogsMagicUtility {

    @Test
    public void testWhylogsMagicUtility() {
        Assert.assertEquals(WhylogsMagicUtility.WHYLOGS_MAGIC_HEADER, "WHY1");
        Assert.assertEquals(WhylogsMagicUtility.WHYLOGS_MAGIC_HEADER_LENGTH, 4);
        Assert.assertEquals(WhylogsMagicUtility.WHYLOGS_MAGIC_HEADER_BYTES, new byte[] {87, 72, 89, 49});
    }
}
