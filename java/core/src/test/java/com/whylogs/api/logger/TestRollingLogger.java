package com.whylogs.api.logger;

import com.whylogs.api.logger.rollingLogger.TimedRollingLogger;
import com.whylogs.core.schemas.DatasetSchema;
import java.util.HashMap;
import org.testng.annotations.Test;

@Test
public class TestRollingLogger {

  @Test
  public void testClosing() {
    HashMap<String, Object> data = createBasicLog();
    TimedRollingLogger logger = new TimedRollingLogger(new DatasetSchema(), "test", ".bin", 1, 'M');
    logger.log(data);
    // TODO: testing needs the writer
  }

  private HashMap<String, Object> createBasicLog() {
    HashMap<String, Object> data = new HashMap<>();
    data.put("col1", 2);
    data.put("col2", 3);
    data.put("col3", 100);

    return data;
  }
}
