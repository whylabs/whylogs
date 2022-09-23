package com.whylogs.api.resultsets;

import com.whylogs.api.logger.resultSets.ProfileResultSet;
import com.whylogs.core.DatasetProfile;

import com.whylogs.core.errors.Error;
import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.schemas.DatasetSchema;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Optional;


@Test
public class TestProfileResultSet {

    private ProfileResultSet defaultResultSet(){
        HashMap<String, Object> data = new HashMap<>();
        data.put("test", 1);
        data.put("test2", "2");
        DatasetSchema datasetSchema = new DatasetSchema();
        datasetSchema.resolve(data);

        DatasetProfile datasetProfile = new DatasetProfile(Optional.of(datasetSchema), Optional.empty(), Optional.empty());
        return new ProfileResultSet(datasetProfile);
    }

    @Test
    public void testProfileResultSet() {
        ProfileResultSet profileResultSet = defaultResultSet();
        Assert.assertNotNull(profileResultSet);
        Assert.assertEquals(profileResultSet.profile().get().getSchema().getColNames().size(), 2);
        Assert.assertEquals(profileResultSet.view().get().getColumns().size(), 2);
        // TODO: BUG HERE IN DATASET SCHEMA Assert.assertEquals(profileResultSet.view().get().getColumns().get("test").getComponents(), 1);


        // Test expected error on unknown column name
        try {
            profileResultSet.addMetric("newTest", IntegralMetric.zero(new MetricConfig()));
        } catch (Error error) {
            Assert.fail("Error adding metric: " + error.getMessage());
        } catch (InputMismatchException e){
            // expected
        }

        //
        try {
            profileResultSet.addMetric("test", IntegralMetric.zero(new MetricConfig()));
        } catch (Error error) {
            Assert.fail("Error adding metric: " + error.getMessage());
        }

        Assert.assertEquals(profileResultSet.view().get().getColumns().size(), 2);
        Assert.assertEquals(profileResultSet.view().get().getColumns().get("test").getComponents().get("MaxIntegralComponent").getValue(), Integer.MIN_VALUE);
    }
}
