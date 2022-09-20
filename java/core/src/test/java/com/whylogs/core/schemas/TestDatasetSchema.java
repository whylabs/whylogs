package com.whylogs.core.schemas;

import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestDatasetSchema {

  @Test
  public void test_dataset_schema() {
    DatasetSchema datasetSchema = new DatasetSchema();
    Assert.assertEquals(datasetSchema.getCache_size(), 1024);

    HashMap<String, Object> data = new HashMap<>();
    data.put("test", 1);
    data.put("test2", "2");
    Assert.assertTrue(datasetSchema.resolve(data));
    Assert.assertEquals(datasetSchema.getColumns().get("test").getType(), Integer.class);
    Assert.assertEquals(datasetSchema.getColumns().get("test2").getType(), String.class);

    Assert.assertFalse(datasetSchema.resolve(data));
  }
}
