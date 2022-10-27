package com.whylogs.api.logger;

import com.whylogs.api.logger.resultsets.ResultSet;
import com.whylogs.core.DatasetProfile;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestLogger {

  @Test
  public void testBasicLog() {
    HashMap<String, Object> data = createBasicLog();

    ResultSet results = new TransientLogger().log(data);
    Assert.assertNotNull(results);

    DatasetProfile profile = results.profile().get();
    Assert.assertNotNull(profile);

    // profile.getColumns().get("col1");
    Assert.assertEquals(profile.getColumns().get("col1").getMetrics().size(), 1);
    Assert.assertEquals(profile.getColumns().get("col1").getSuccessCount(), 1);
    Assert.assertEquals(profile.getColumns().get("col1").getSchema().getType(), Integer.class);
  }

  private HashMap<String, Object> createBasicLog() {
    HashMap<String, Object> data = new HashMap<>();
    data.put("col1", 2);
    data.put("col2", 3);
    data.put("col3", 100);

    return data;
  }
}
