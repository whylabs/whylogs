package com.whylogs.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Int64Value;
import com.whylogs.core.message.InferredType;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.testng.annotations.Test;

public class ColumnProfileTest {
  @Test
  public void column_BasicTracking_ShouldWork() {
    val col = new ColumnProfile("test");
    col.track(1L);
    col.track(1.0);
    col.track("string");
    col.track(true);
    col.track(false);
    col.track(null);

    assertThat(col.getCounters().getCount(), is(6L));
    assertThat(col.getSchemaTracker().getTypeCounts().get(InferredType.Type.NULL), is(1L));
    assertThat(col.getCounters().getTrueCount(), is(1L));
    assertThat(col.getNumberTracker().getLongs().getCount(), is(0L));
    assertThat(col.getNumberTracker().getDoubles().getCount(), is(2L));
  }

  /** Check that custom null specification detects nulls and has no false positives. */
  @Test
  public void column_NullTest_ShouldWork() {
    val col = new ColumnProfile("test", ImmutableSet.of("nil", "NaN", "nan", "null"));
    col.track(1L);
    col.track(1.0);
    col.track("string");
    col.track(true);
    col.track(false);
    col.track(null);
    col.track(Double.NaN);
    col.track(Double.NEGATIVE_INFINITY);
    col.track(Double.POSITIVE_INFINITY);
    col.track("");

    assertThat(col.getCounters().getCount(), is(10L));
    assertThat(col.getSchemaTracker().getTypeCounts().get(InferredType.Type.NULL), is(4L));
    assertThat(col.getCounters().getTrueCount(), is(1L));
    assertThat(col.getNumberTracker().getLongs().getCount(), is(0L));
    assertThat(col.getNumberTracker().getDoubles().getCount(), is(2L));
  }

  @Test
  public void column_track_NaN() {
    val col = new ColumnProfile("test");
    col.track(Double.NaN);

    assertThat(col.getCounters().getCount(), is(1L));
    assertThat(col.getSchemaTracker().getTypeCounts().get(InferredType.Type.NULL), is(1L));
    assertThat(col.getCounters().getTrueCount(), is(0L));
    assertThat(col.getNumberTracker().getLongs().getCount(), is(0L));
    assertThat(col.getNumberTracker().getDoubles().getCount(), is(0L));
  }

  @Test
  public void column_Merge_Success() {
    val col = new ColumnProfile("test");
    col.track(1L);
    col.track(1.0);
    col.track("string");
    col.track(true);
    col.track(false);
    col.track(null);

    val merged = col.merge(col);
    assertThat(merged.getCounters().getCount(), is(12L));
    assertThat(merged.getSchemaTracker().getTypeCounts().get(InferredType.Type.NULL), is(2L));
    assertThat(merged.getCounters().getTrueCount(), is(2L));
    assertThat(merged.getNumberTracker().getLongs().getCount(), is(0L));
    assertThat(merged.getNumberTracker().getDoubles().getCount(), is(4L));

    // verify that the merged profile is updatable
    merged.track("value");
  }

  @Test
  public void column_Merge_RetainCommonTags() {
    val col = new ColumnProfile("test");
    col.track(1L);
    col.track(1.0);
    col.track("string");
    col.track(true);
    col.track(false);
    col.track(null);

    val merged = col.merge(col);
    assertThat(merged.getCounters().getCount(), is(12L));
    assertThat(merged.getSchemaTracker().getTypeCounts().get(InferredType.Type.NULL), is(2L));
    assertThat(merged.getCounters().getTrueCount(), is(2L));
    assertThat(merged.getNumberTracker().getLongs().getCount(), is(0L));
    assertThat(merged.getNumberTracker().getDoubles().getCount(), is(4L));

    // verify that the merged profile is updatable
    merged.track("value");
  }

  /*
   Tests the merge of column profile with stringtracker to legacy column without stringtracker
  */
  @Test
  @SneakyThrows
  public void column_Merge_StringTracker() {

    val col = new ColumnProfile("fico_range_low");
    col.track("But, soft! what light through yonder window breaks?");
    {
      //  test merging stringtracker with itself.
      val merged = col.merge(col);
      assertThat(merged.getStringTracker().getCount(), is(2L));
      assertThat(merged.getStringTracker().getLength().getLongs().getMax(), is(51L));
      assertThat(merged.getStringTracker().getTokenLength().getLongs().getMax(), is(8L));
    }
    {
      //  test merging with legacy column that does not have stringtracker
      val ds = DatasetProfile.parse(getClass().getResourceAsStream("/python_profile.bin"));
      val col2 = ds.columns.get("fico_range_low");
      val merged = col.merge(col2);
      assertThat(merged.getStringTracker().getCount(), is(1L));
      assertThat(merged.getStringTracker().getLength().getLongs().getMax(), is(51L));
      assertThat(merged.getStringTracker().getTokenLength().getLongs().getMax(), is(8L));
    }
  }

  @Test
  public void maxStringTruncation() {
    val col = new ColumnProfile("test");
    col.track(
        "superlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstring");

    val merged = col.merge(col);
    val r = merged.getFrequentItems().getFrequentItems(1, ErrorType.NO_FALSE_POSITIVES);
    assertThat(
        "String should not exceed the limit",
        r[0].getItem().length() <= ColumnProfile.STRING_LENGTH_MAX);

    // verify that the merged profile is updatable
    merged.track("value");
  }

  @Test
  public void profile_numeric_strings() {
    val col = new ColumnProfile("test");

    for (int i = 0; i < 1000; i++) {
      col.track(String.valueOf(i));
    }
    assertThat(col.getFrequentItems().getNumActiveItems(), is(lessThan(40)));
    for (ItemsSketch.Row<String> item :
        col.getFrequentItems().getFrequentItems(ErrorType.NO_FALSE_NEGATIVES)) {
      // attempt to parse all the values
      //noinspection ResultOfMethodCallIgnored
      Integer.parseInt(item.getItem());
    }
  }

  @Test
  public void profile_numeric_values() {
    val col = new ColumnProfile("test");

    for (int i = 0; i < 1000; i++) {
      col.track(i);
    }
    assertThat(col.getFrequentItems().getNumActiveItems(), is(lessThan(40)));
    for (ItemsSketch.Row<String> item :
        col.getFrequentItems().getFrequentItems(ErrorType.NO_FALSE_NEGATIVES)) {
      // attempt to parse all the values
      //noinspection ResultOfMethodCallIgnored
      Integer.parseInt(item.getItem());
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void column_Merge_Failure() {
    val col = new ColumnProfile("foo");
    val other = new ColumnProfile("bar");
    col.merge(other);
  }

  @Test
  public void protobuf_RoundTripSerialization_Success() {
    val original = new ColumnProfile("test");
    original.track(1L);
    original.track(1.0);

    val msg = original.toProtobuf().build();
    val roundTrip = ColumnProfile.fromProtobuf(msg);

    assertThat(roundTrip.getColumnName(), is("test"));
    assertThat(roundTrip.getCounters().getCount(), is(2L));
  }

  /** Assert summary quantiles are not empty. */
  @Test
  public void colums_Summary_Success() {
    val col = new ColumnProfile("test");
    col.track(1L);
    col.track(1.0);

    val quantiles = col.toColumnSummary().getNumberSummary().getQuantiles();
    assertThat(quantiles.getQuantilesCount(), is(9));
  }

  @Test
  public void handle_legacy_null_count() {
    val original = new ColumnProfile("test");
    original.track(1L);
    original.track(1.0);
    original.track(null);

    val msgBuilder = original.toProtobuf();
    val legacyCounterWithNullCount =
        msgBuilder
            .getCounters()
            .toBuilder()
            .setNullCount(Int64Value.newBuilder().setValue(2L).build());
    msgBuilder.setCounters(legacyCounterWithNullCount);
    val msg = msgBuilder.build();

    val roundTrip = ColumnProfile.fromProtobuf(msg);

    assertThat(roundTrip.getColumnName(), is("test"));
    assertThat(roundTrip.getCounters().getCount(), is(3L));
    // ensure that the null count is a sum
    assertThat(roundTrip.getSchemaTracker().getTypeCounts().get(InferredType.Type.NULL), is(3L));
  }
}
