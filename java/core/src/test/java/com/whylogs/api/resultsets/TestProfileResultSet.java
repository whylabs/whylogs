package com.whylogs.api.resultsets;

import com.whylogs.api.logger.resultSets.ProfileResultSet;
import com.whylogs.core.DatasetProfile;
import com.whylogs.core.errors.Error;
import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.schemas.DatasetSchema;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestProfileResultSet {

  private ProfileResultSet defaultResultSet() {
    HashMap<String, Object> data = new HashMap<>();
    data.put("test", 1);
    data.put("test2", "2");
    DatasetSchema datasetSchema = new DatasetSchema();
    datasetSchema.resolve(data);

    DatasetProfile datasetProfile =
        new DatasetProfile(Optional.of(datasetSchema), Optional.empty(), Optional.empty());
    return new ProfileResultSet(datasetProfile);
  }

  @Test
  public void testProfileResultSet() {
    ProfileResultSet profileResultSet = defaultResultSet();
    Assert.assertNotNull(profileResultSet);

    if (profileResultSet.profile().isPresent()) {
      DatasetProfile datasetProfile = profileResultSet.profile().get();
      Assert.assertNotNull(datasetProfile);
      Assert.assertEquals(datasetProfile.getSchema().getColumns().size(), 2);
    } else {
      Assert.fail("Profile is not present");
    }

    if (profileResultSet.view().isPresent()) {
      Assert.assertEquals(profileResultSet.view().get().getColumns().size(), 2);
      Assert.assertEquals(
          profileResultSet.view().get().getColumns().get("test").getComponents().size(), 2);
    } else {
      Assert.fail("View is not present");
    }

    // Test expected error on unknown column name
    try {
      profileResultSet.addMetric("newTest", IntegralMetric.zero(new MetricConfig()));
    } catch (Error error) {
      Assert.fail("Error adding metric: " + error.getMessage());
    } catch (InputMismatchException e) {
      // expected
    }

    //
    try {
      profileResultSet.addMetric("test", IntegralMetric.zero(new MetricConfig()));
    } catch (Error error) {
      Assert.fail("Error adding metric: " + error.getMessage());
    }

    Assert.assertEquals(profileResultSet.view().get().getColumns().size(), 2);
    Assert.assertEquals(
        profileResultSet
            .view()
            .get()
            .getColumns()
            .get("test")
            .getComponents()
            .get("MaxIntegralComponent")
            .getValue(),
        Integer.MIN_VALUE);
  }
}
