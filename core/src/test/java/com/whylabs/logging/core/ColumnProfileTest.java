package com.whylabs.logging.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import lombok.val;
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
    assertThat(col.getCounters().getNullCount(), is(1L));
    assertThat(col.getCounters().getTrueCount(), is(1L));
    assertThat(col.getNumberTracker().getLongs().getCount(), is(0L));
    assertThat(col.getNumberTracker().getDoubles().getCount(), is(2L));
    assertThat(col.getStringTracker().getCount(), is(1L));
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
    assertThat(merged.getCounters().getNullCount(), is(2L));
    assertThat(merged.getCounters().getTrueCount(), is(2L));
    assertThat(merged.getNumberTracker().getLongs().getCount(), is(0L));
    assertThat(merged.getNumberTracker().getDoubles().getCount(), is(4L));
    assertThat(merged.getStringTracker().getCount(), is(2L));
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
}
