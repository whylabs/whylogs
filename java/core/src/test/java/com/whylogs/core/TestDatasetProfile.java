package com.whylogs.core;

import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.schemas.DatasetSchema;
import com.whylogs.core.views.DatasetProfileView;
import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestDatasetProfile {
  private Instant creationTime;
  private Instant datasetTime;

  private DatasetSchema defaultSchema() {
    DatasetSchema datasetSchema = new DatasetSchema();
    HashMap<String, Object> data = new HashMap<>();
    // TODO: Double check this data schema that it's working as expected
    data.put("test", 1);
    data.put("test2", "2");
    Assert.assertTrue(datasetSchema.resolve(data));
    return datasetSchema;
  }

  private DatasetProfile defaultProfile() {
    return new DatasetProfile(Optional.of(defaultSchema()), Optional.empty(), Optional.empty());
  }

  private DatasetProfile customTimeZone() {
    return new DatasetProfile(
        Optional.of(defaultSchema()),
        Optional.ofNullable(creationTime),
        Optional.ofNullable(datasetTime));
  }

  @Test
  public void testDatasetTimes() {
    DatasetProfile profile = customTimeZone();
    creationTime = Instant.now();
    datasetTime = Instant.now();
    Assert.assertEquals(
        profile.getCreationTimestamp().getEpochSecond(), creationTime.getEpochSecond());
    Assert.assertEquals(
        profile.getDatasetTimestamp().getEpochSecond(), datasetTime.getEpochSecond());

    datasetTime = Instant.now();
    profile.setDatasetTimestamp(datasetTime);
    Assert.assertEquals(
        profile.getDatasetTimestamp().getEpochSecond(), datasetTime.getEpochSecond());
  }

  @Test
  public void testDatasetProfileInit() {
    DatasetProfile profile = defaultProfile();
    Assert.assertEquals(profile.getColumns().size(), 2);
    Assert.assertEquals(profile.getColumns().get("test").getFailureCount(), 0);

    Assert.assertTrue(DatasetProfile.getDefaultPath(Optional.of("test")).contains("test_profile"));
    Assert.assertEquals(profile.getSchema().getColumns().size(), 2);
    Assert.assertEquals(
        profile.getSchema().getColumns().get("test").getMetrics().size(),
        1); // THere should only be the IntegralMetric
  }

  @Test
  public void testAddMetric() {
    DatasetProfile profile = defaultProfile();
    profile.addMetric("test", IntegralMetric.zero());
    Assert.assertEquals(profile.getColumns().get("test").getMetrics().size(), 1);
    Assert.assertEquals(profile.getTrackCount(), 0); // Because we added directly
  }

  @Test
  public void testTrackData() {
    DatasetProfile profile = defaultProfile();
    HashMap<String, Object> data = new HashMap<>();
    data.put("test", 1);
    data.put("test2", "2");
    profile.track(data);

    Assert.assertEquals(profile.getColumns().get("test").getSuccessCount(), 1);
    Assert.assertEquals(profile.getTrackCount(), 1);
  }

  @Test
  public void testTrackNullDate() {
    DatasetProfile profile = defaultProfile();
    HashMap<String, Object> data = new HashMap<>();
    data.put("test", null);
    data.put("test2", "2");
    profile.track(data);

    Assert.assertEquals(profile.getColumns().get("test").getCachedValues().size(), 1);
    Assert.assertEquals(profile.getTrackCount(), 1);
  }

  @Test
  public void testDirty() {
    DatasetProfile profile = defaultProfile();
    HashMap<String, Object> data = new HashMap<>();
    data.put("notSeen", 100);
    profile.track(data);

    Assert.assertEquals(profile.getColumns().get("notSeen").getSuccessCount(), 1);
    Assert.assertEquals(profile.getTrackCount(), 1);
  }

  public void testView() {
    DatasetProfile profile = defaultProfile();
    DatasetProfileView view = profile.view();
    Assert.assertEquals(view.getColumns().size(), 2);
    Assert.assertEquals(view.getColumns().get("test").getMetrics().size(), 0);
  }

  public void testFlush() {
    DatasetProfile profile = defaultProfile();
    HashMap<String, Object> data = new HashMap<>();
    data.put("test", 1);
    data.put("test2", "2");
    profile.track(data);
    profile.flush();
  }
}
