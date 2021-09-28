package com.whylogs.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.whylogs.core.message.ModelType;
import com.whylogs.core.metrics.RegressionMetrics;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import lombok.val;
import org.apache.commons.lang3.SerializationUtils;
import org.testng.annotations.Test;

public class DatasetProfileTest {
  @Test
  public void merge_EmptyValidDatasetProfiles_EmptyResult() {
    final Instant now = Instant.now();
    val first = new DatasetProfile("test", now);
    val second = new DatasetProfile("test", now);

    val result = first.merge(second);
    assertThat(result.getSessionId(), is("test"));
    assertThat(result.getSessionTimestamp(), is(now));
    assertThat(result.columns, is(anEmptyMap()));
  }

  @Test
  public void merge_DifferentColumns_ColumnsAreMerged() {
    final Instant now = Instant.now();
    val first = new DatasetProfile("test", now, ImmutableMap.of("key", "value"));
    first.track("col1", "value");
    val second = new DatasetProfile("test", now, ImmutableMap.of("key", "value"));
    second.track("col2", "value");

    val result = first.merge(second);
    assertThat(result.getSessionId(), is("test"));
    assertThat(result.getSessionTimestamp(), is(now));
    assertThat(result.columns, aMapWithSize(2));
    assertThat(result.columns, hasKey("col1"));
    assertThat(result.columns, hasKey("col2"));
    assertThat(result.tags, aMapWithSize(1));
    assertThat(result.tags.values(), contains("value"));

    // verify counters
    assertThat(result.columns.get("col1").getCounters().getCount(), is(1L));
    assertThat(result.columns.get("col2").getCounters().getCount(), is(1L));
  }

  @Test
  public void merge_SameColumns_ColumnsAreMerged() {
    final Instant now = Instant.now();
    val first = new DatasetProfile("test", now);
    first.track("col1", "value1");
    val second = new DatasetProfile("test", now);
    second.track("col1", "value1");
    second.track("col2", "value");

    val result = first.merge(second);
    assertThat(result.getSessionId(), is("test"));
    assertThat(result.getSessionTimestamp(), is(now));
    assertThat(result.columns, aMapWithSize(2));
    assertThat(result.columns, hasKey("col1"));
    assertThat(result.columns, hasKey("col2"));

    // verify counters
    assertThat(result.columns.get("col1").getCounters().getCount(), is(2L));
    assertThat(result.columns.get("col2").getCounters().getCount(), is(1L));
  }

  @Test
  public void merge_DifferentColumns_WithDataTimestamp_ColumnsAreMerged() {
    val now = Instant.now();
    val dataTimestamp = now.truncatedTo(ChronoUnit.DAYS);
    val first =
        new DatasetProfile(
            "test", now, dataTimestamp, ImmutableMap.of("key", "value"), Collections.emptyMap());
    first.track("col1", "value");
    val second =
        new DatasetProfile(
            "test", now, dataTimestamp, ImmutableMap.of("key", "value"), Collections.emptyMap());
    second.track("col2", "value");

    val result = first.merge(second);
    assertThat(result.getSessionId(), is("test"));
    assertThat(result.getSessionTimestamp(), is(now));
    assertThat(result.columns, aMapWithSize(2));
    assertThat(result.columns, hasKey("col1"));
    assertThat(result.columns, hasKey("col2"));
    assertThat(result.tags, aMapWithSize(1));
    assertThat(result.tags.values(), contains("value"));
    assertThat(result.getDataTimestamp(), is(dataTimestamp));
    assertThat(result.getSessionTimestamp(), is(now));

    // verify counters
    assertThat(result.columns.get("col1").getCounters().getCount(), is(1L));
    assertThat(result.columns.get("col2").getCounters().getCount(), is(1L));
  }

  @Test
  public void merge_ContainsDifferentTagGroups_ShouldRetainCommonTags() {
    val now = Instant.now();
    val first = new DatasetProfile("test", now, ImmutableMap.of("key", "foo", "key2", "foo2"));
    first.withMetadata("m1", "v1").withMetadata("m2", "v2").withMetadata("m0", "v0");
    val second =
        new DatasetProfile(
            "test", now.plusSeconds(60), ImmutableMap.of("key", "foo", "key2", "foo3"));
    second.withMetadata("m1", "v1").withMetadata("m2", "v2").withMetadata("m3", "v3");

    val result = first.merge(second);
    assertThat(result.getTags(), is(ImmutableMap.of("key", "foo")));
    assertThat(result.getMetadata(), is(ImmutableMap.of("m1", "v1", "m2", "v2")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void mergeStrict_MismatchedTags_ThrowsIllegalArgumentException() {
    val now = Instant.now();
    val first = new DatasetProfile("test", now, ImmutableMap.of("key", "foo"));
    val second = new DatasetProfile("test", now, ImmutableMap.of("key", "bar"));

    first.mergeStrict(second);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void mergeStrict_MismatchedSessionTime_ThrowsIllegalArgumentException() {
    val now = Instant.now();
    val first = new DatasetProfile("test", now);
    val second = new DatasetProfile("test", now.minusMillis(1));

    first.mergeStrict(second);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void mergeStrict_MismatchedDataTime_ThrowsIllegalArgumentException() {
    val now = Instant.now();
    val first =
        new DatasetProfile(
            "test", now, now.minusMillis(1), Collections.emptyMap(), Collections.emptyMap());
    val second =
        new DatasetProfile(
            "test", now, now.plusMillis(1), Collections.emptyMap(), Collections.emptyMap());

    first.mergeStrict(second);
  }

  @Test
  public void protobuf_RoundTripSerialization_Success() {
    val sessionTime = Instant.now();
    val dataTime = Instant.now().truncatedTo(ChronoUnit.DAYS);
    val tags = ImmutableMap.of("key1", "rock", "key2", "scissors", "key3", "paper");

    val original = new DatasetProfile("test", sessionTime, dataTime, tags, Collections.emptyMap());
    original.track("col1", "value");
    original.track("col2", "value");
    original.withMetadata("mKey", "mData");

    val msg = original.toProtobuf().build();
    val roundTrip = DatasetProfile.fromProtobuf(msg);

    assertThat(roundTrip.getSessionId(), is("test"));
    assertThat(roundTrip.getDataTimestamp(), is(dataTime));
    assertThat(roundTrip.getSessionTimestamp().toEpochMilli(), is(sessionTime.toEpochMilli()));
    assertThat(roundTrip.columns, aMapWithSize(2));
    assertThat(roundTrip.tags, aMapWithSize(3));
    assertThat(roundTrip.modelProfile, is(nullValue()));
    assertThat(roundTrip.tags.values(), containsInAnyOrder("paper", "rock", "scissors"));
    assertThat(roundTrip.columns.get("col1").getCounters().getCount(), is(1L));
    assertThat(roundTrip.columns.get("col2").getCounters().getCount(), is(1L));
  }

  @Test
  public void javaSerialization_RoundTripWithDataTime_Success() {
    val sessionTime = Instant.now();
    val dataTime = Instant.now().truncatedTo(ChronoUnit.DAYS);
    val tags = ImmutableMap.of("key1", "rock", "key2", "scissors", "key3", "paper");
    val original = new DatasetProfile("test", sessionTime, dataTime, tags, Collections.emptyMap());
    original.withMetadata("mKey", "mData");

    original.track("col1", "value");
    original.track("col1", 1);
    original.track("col2", "value");

    val roundTrip = SerializationUtils.clone(original);
    assertThat(roundTrip.getSessionId(), is("test"));
    assertThat(roundTrip.getSessionTimestamp().toEpochMilli(), is(sessionTime.toEpochMilli()));
    assertThat(roundTrip.getDataTimestamp(), is(dataTime));
    assertThat(roundTrip.columns, aMapWithSize(2));
    assertThat(roundTrip.tags, aMapWithSize(3));
    assertThat(roundTrip.tags.values(), containsInAnyOrder("paper", "rock", "scissors"));
    assertThat(roundTrip.metadata.get("mKey"), is("mData"));
  }

  @Test
  public void javaSerialization_RoundTripWithMissingDataTime_Success() {
    val sessionTime = Instant.now();
    val tags = ImmutableMap.of("key1", "rock", "key2", "scissors", "key3", "paper");
    val original = new DatasetProfile("test", sessionTime, null, tags, Collections.emptyMap());
    original.withMetadata("mKey", "mData");

    original.track("col1", "value");
    original.track("col1", 1);
    original.track("col2", "value");

    val roundTrip = SerializationUtils.clone(original);
    assertThat(roundTrip.getSessionId(), is("test"));
    assertThat(roundTrip.getSessionTimestamp().toEpochMilli(), is(sessionTime.toEpochMilli()));
    assertThat(roundTrip.getDataTimestamp(), nullValue());
    assertThat(roundTrip.columns, aMapWithSize(2));
    assertThat(roundTrip.tags, aMapWithSize(3));
    assertThat(roundTrip.tags.values(), containsInAnyOrder("paper", "rock", "scissors"));
    assertThat(roundTrip.metadata.get("mKey"), is("mData"));
  }

  @Test
  public void javaSerialization_RoundTripWithModelProfile_Success() {
    val sessionTime = Instant.now();
    val tags = ImmutableMap.of("key1", "rock", "key2", "scissors", "key3", "paper");
    val original =
        new DatasetProfile("test", sessionTime, null, tags, Collections.emptyMap())
            .withClassificationModel(
                "pred", "target", "score", ImmutableList.of("additionalOutput"));

    original.track("col1", "value");
    original.track("col1", 1);
    original.track("col2", "value");

    val roundTrip = SerializationUtils.clone(original);
    assertThat(roundTrip.getSessionId(), is("test"));
    assertThat(roundTrip.getSessionTimestamp().toEpochMilli(), is(sessionTime.toEpochMilli()));
    assertThat(roundTrip.getDataTimestamp(), nullValue());
    assertThat(roundTrip.columns, aMapWithSize(2));
    assertThat(roundTrip.tags, aMapWithSize(3));
    assertThat(roundTrip.tags.values(), containsInAnyOrder("paper", "rock", "scissors"));
    assertThat(roundTrip.modelProfile, is(notNullValue()));
    assertThat(
        roundTrip.modelProfile.getMetrics().getClassificationMetrics().getPredictionField(),
        is("pred"));
    assertThat(
        roundTrip.modelProfile.getMetrics().getClassificationMetrics().getTargetField(),
        is("target"));
    assertThat(
        roundTrip.modelProfile.getMetrics().getClassificationMetrics().getScoreField(),
        is("score"));
  }

  @Test
  public void deserialization_should_succeed() throws IOException {
    DatasetProfile.parse(getClass().getResourceAsStream("/python_profile.bin"));
  }

  @Test
  public void deserialization_withRegressionData() throws IOException {
    val profile = DatasetProfile.parse(getClass().getResourceAsStream("/regression.bin"));
    final RegressionMetrics regressionMetrics =
        profile.modelProfile.getMetrics().getRegressionMetrics();
    assertThat(regressionMetrics, is(notNullValue()));
    assertThat(regressionMetrics.getCount(), is(89L));
  }

  @Test
  public void testMergeRecentWithOlderProfile() throws IOException {
    val profile1 = DatasetProfile.parse(getClass().getResourceAsStream("/python_profile.bin"));
    val profile2 = DatasetProfile.parse(getClass().getResourceAsStream("/regression.bin"));
    DatasetProfile merged = profile1.merge(profile2);
    merged = DatasetProfile.fromProtobuf(merged.toProtobuf().build());
    assertNotNull(merged.getModelProfile());
    assertNotNull(merged.getModelProfile().getMetrics());
    assertEquals(merged.getModelProfile().getMetrics().getModelType(), ModelType.REGRESSION);
    assertMetrics(merged.getModelProfile().getMetrics().getRegressionMetrics(), 1);
  }

  @Test
  public void testMergeRecentWithOlderProfileOppositeDirection() throws IOException {
    val profile1 = DatasetProfile.parse(getClass().getResourceAsStream("/python_profile.bin"));
    val profile2 = DatasetProfile.parse(getClass().getResourceAsStream("/regression.bin"));
    DatasetProfile merged = profile2.merge(profile1);
    merged = DatasetProfile.fromProtobuf(merged.toProtobuf().build());
    assertNotNull(merged.getModelProfile());
    assertNotNull(merged.getModelProfile().getMetrics());
    assertMetrics(merged.getModelProfile().getMetrics().getRegressionMetrics(), 1);
  }

  private void assertMetrics(RegressionMetrics metrics, int multiplier) {
    assertThat(metrics.getSumAbsDiff(), closeTo(7649.135452245152 * multiplier, 0.01));
    assertThat(metrics.getSumDiff(), closeTo(522.7580608276942 * multiplier, 0.01));
    assertThat(metrics.getSum2Diff(), closeTo(1021265.7543864828 * multiplier, 0.01));
    assertEquals(metrics.getCount(), 89 * multiplier);
    assertEquals(metrics.getTargetField(), "targets");
    assertEquals(metrics.getPredictionField(), "predictions");
  }

  @Test
  public void testMergeTwoNewerProfiles() throws IOException {
    val profile1 = DatasetProfile.parse(getClass().getResourceAsStream("/regression.bin"));
    val profile2 = DatasetProfile.parse(getClass().getResourceAsStream("/regression.bin"));
    DatasetProfile merged = profile1.merge(profile2);
    merged = DatasetProfile.fromProtobuf(merged.toProtobuf().build());
    assertNotNull(merged.getModelProfile());
    assertNotNull(merged.getModelProfile().getMetrics());
    assertMetrics(merged.getModelProfile().getMetrics().getRegressionMetrics(), 2);
  }

  @Test
  public void testMergeTwoOlderProfiles() throws IOException {
    val profile1 = DatasetProfile.parse(getClass().getResourceAsStream("/python_profile.bin"));
    val profile2 = DatasetProfile.parse(getClass().getResourceAsStream("/python_profile.bin"));
    DatasetProfile merged = profile1.merge(profile2);
    merged = DatasetProfile.fromProtobuf(merged.toProtobuf().build());
    assertNull(merged.getModelProfile());
  }

  @Test
  public void testMergeTwoLegacyProfiles() throws IOException {
    val profile1 = DatasetProfile.parse(getClass().getResourceAsStream("/profiles-1.bin"));
    val profile2 = DatasetProfile.parse(getClass().getResourceAsStream("/profiles-1.bin"));
    DatasetProfile merged = profile1.merge(profile2);
    merged = DatasetProfile.fromProtobuf(merged.toProtobuf().build());
    assertNull(merged.getModelProfile());
  }

  @Test
  public void testModelMetricsWithUnknownTypeSpecified() throws IOException {
    DatasetProfile profile1 =
        DatasetProfile.parse(getClass().getResourceAsStream("/profiles-1.bin"));
    val profile2 = DatasetProfile.parse(getClass().getResourceAsStream("/profiles-1.bin"));
    // .getModelProfile().getMetrics().getModelType();
    val builder = profile1.toProtobuf().build().toBuilder();
    val metrics = new RegressionMetrics("prediction", "target");
    metrics.track(ImmutableMap.of("prediction", 1, "target", 1));
    // Model type must be specified if any metrics are present. This can only occur when validation
    // between
    // languages isn't 1:1 for creating and reading a profile
    val metricsBuilder =
        builder
            .getModeProfile()
            .getMetrics()
            .toBuilder()
            .setModelType(ModelType.UNKNOWN)
            .setRegressionMetrics(metrics.toProtobuf());
    val modelProfile =
        builder.getModeProfile().toBuilder().setMetrics(metricsBuilder.build()).build();
    val unkown = builder.setModeProfile(modelProfile).build();
    val a = DatasetProfile.fromProtobuf(unkown);
    val b = DatasetProfile.fromProtobuf(unkown);
    a.merge(b).toProtobuf();
  }

  @Test
  public void testMergeLegacyProfilesWithModelMetricsProfile() throws IOException {
    val profile1 = DatasetProfile.parse(getClass().getResourceAsStream("/profiles-1.bin"));
    val profile2 = DatasetProfile.parse(getClass().getResourceAsStream("/regression.bin"));
    DatasetProfile merged = profile1.merge(profile2);
    merged = DatasetProfile.fromProtobuf(merged.toProtobuf().build());
    assertMetrics(merged.getModelProfile().getMetrics().getRegressionMetrics(), 1);
  }

  @Test
  public void testMergeLegacyProfilesWithModelMetricsProfileOppositeDirection() throws IOException {
    val profile1 = DatasetProfile.parse(getClass().getResourceAsStream("/profiles-1.bin"));
    val profile2 = DatasetProfile.parse(getClass().getResourceAsStream("/regression.bin"));
    DatasetProfile merged = profile2.merge(profile1);
    merged = DatasetProfile.fromProtobuf(merged.toProtobuf().build());
    assertMetrics(merged.getModelProfile().getMetrics().getRegressionMetrics(), 1);
  }

  @Test
  public void roundTripWithModelData_should_succeed() {
    val dp =
        new DatasetProfile("test", Instant.now())
            .withClassificationModel("pred", "target", "score");
    dp.track(ImmutableMap.of("pred", 1, "target", 1, "score", 0.5));
    dp.track(ImmutableMap.of("pred", 1, "target", 0, "score", 0.5));
    val msg = dp.toProtobuf().build();
    val roundTrip = DatasetProfile.fromProtobuf(msg);
    assertThat(roundTrip.getModelProfile(), is(notNullValue()));
    assertThat(
        roundTrip.getModelProfile().getMetrics().getClassificationMetrics().getPredictionField(),
        is("pred"));
    assertThat(
        roundTrip.getModelProfile().getMetrics().getClassificationMetrics().getTargetField(),
        is("target"));
    assertThat(
        roundTrip.getModelProfile().getMetrics().getClassificationMetrics().getScoreField(),
        is("score"));
  }

  @Test
  public void test_mergeOld_WithNew() throws IOException {
    // by reading a profile, we basically "convert" it to the new format
    val profile1 = DatasetProfile.parse(getClass().getResourceAsStream("/python_profile.bin"));
    DatasetProfile.fromProtobuf(profile1.toProtobuf().build()).merge(profile1);

    val profile2 = DatasetProfile.parse(getClass().getResourceAsStream("/regression.bin"));
    DatasetProfile.fromProtobuf(profile2.toProtobuf().build()).merge(profile2);
  }
}
