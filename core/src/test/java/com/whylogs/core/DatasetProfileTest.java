package com.whylogs.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableMap;
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

    final val result = first.merge(second);
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

    final val result = first.merge(second);
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

    final val result = first.merge(second);
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

    final val result = first.merge(second);
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

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void merge_MismatchedTags_ThrowsIllegalArgumentException() {
    val now = Instant.now();
    val first = new DatasetProfile("test", now, ImmutableMap.of("key", "foo"));
    val second = new DatasetProfile("test", now, ImmutableMap.of("key", "bar"));

    first.merge(second);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void merge_MismatchedSessionTime_ThrowsIllegalArgumentException() {
    val now = Instant.now();
    val first = new DatasetProfile("test", now);
    val second = new DatasetProfile("test", now.minusMillis(1));

    first.merge(second);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void merge_MismatchedDataTime_ThrowsIllegalArgumentException() {
    val now = Instant.now();
    val first =
        new DatasetProfile(
            "test", now, now.minusMillis(1), Collections.emptyMap(), Collections.emptyMap());
    val second =
        new DatasetProfile(
            "test", now, now.plusMillis(1), Collections.emptyMap(), Collections.emptyMap());

    first.merge(second);
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

    final val msg = original.toProtobuf().build();
    final val roundTrip = DatasetProfile.fromProtobuf(msg);

    assertThat(roundTrip.getSessionId(), is("test"));
    assertThat(roundTrip.getDataTimestamp(), is(dataTime));
    assertThat(roundTrip.getSessionTimestamp(), is(sessionTime));
    assertThat(roundTrip.columns, aMapWithSize(2));
    assertThat(roundTrip.tags, aMapWithSize(3));
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
    assertThat(roundTrip.getSessionTimestamp(), is(sessionTime));
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
    assertThat(roundTrip.getSessionTimestamp(), is(sessionTime));
    assertThat(roundTrip.getDataTimestamp(), nullValue());
    assertThat(roundTrip.columns, aMapWithSize(2));
    assertThat(roundTrip.tags, aMapWithSize(3));
    assertThat(roundTrip.tags.values(), containsInAnyOrder("paper", "rock", "scissors"));
    assertThat(roundTrip.metadata.get("mKey"), is("mData"));
  }
}
