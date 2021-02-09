package com.whylogs.core.statistics.datatypes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import lombok.val;
import org.testng.annotations.Test;

public class StringTrackerTest {

  @Test
  public void stringTracker_BasicStringTracking() {
    val tracker = new StringTracker();
    tracker.update("foo1");
    tracker.update("foo2");
    tracker.update("foo3");

    assertThat(tracker.getCount(), is(3L));
    assertThat(tracker.getItems().getNumActiveItems(), is(3));
    assertThat(tracker.getThetaSketch().getResult().getEstimate(), is(3.0));
  }

  @Test
  public void stringTracker_Merge() {
    val tracker = new StringTracker();
    tracker.update("foo1");
    tracker.update("foo2");
    tracker.update("foo3");

    assertThat(tracker.getCount(), is(3L));
    assertThat(tracker.getItems().getNumActiveItems(), is(3));

    val merged = tracker.merge(tracker);
    assertThat(merged.getCount(), is(6L));
    assertThat(merged.getItems().getNumActiveItems(), is(3));

    // verify we can serialize the merge object
    val msg = merged.toProtobuf().build();
    StringTracker.fromProtobuf(msg);
  }

  @Test
  public void protobuf_Roundtrip_Serialization() {
    val original = new StringTracker();
    original.update("foo1");
    original.update("foo2");
    original.update("foo3");

    val msg = original.toProtobuf().build();
    val roundtrip = StringTracker.fromProtobuf(msg);
    assertThat(roundtrip.getCount(), is(3L));
  }
}
