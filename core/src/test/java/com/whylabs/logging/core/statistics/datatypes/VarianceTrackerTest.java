package com.whylabs.logging.core.statistics.datatypes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import lombok.val;
import org.testng.annotations.Test;

public class VarianceTrackerTest {

  @Test
  public void update_BasicCases_ShouldBeCorrect() {
    val tracker = new VarianceTracker();
    tracker.update(1.0);
    tracker.update(2.0);
    tracker.update(3.0);

    assertThat(tracker.getMean(), closeTo(2.0, 0.0001));
    assertThat(tracker.variance(), closeTo(1.0, 0.0001));
    assertThat(tracker.getCount(), is(3L));
  }

  @Test
  public void merge_SimpleTrackers_ShouldBeCorrect() {
    val first = new VarianceTracker();
    first.update(1.0);

    val second = new VarianceTracker();
    second.update(2.0);
    second.update(3.0);

    val merged = first.merge(second);
    assertThat(merged.variance(), closeTo(1.0, 0.0001));
    assertThat(merged.getMean(), closeTo(2.0, 0.0001));
    assertThat(merged.getCount(), is(3L));
  }

  @Test
  public void merge_WithAnotherEmptyTracker_ShouldBeCorrect() {
    val first = new VarianceTracker();
    // add a series of values [0,1,...,9]
    for (int i = 0; i < 10; i++) {
      first.update(i);
    }
    assertThat(first.variance(), closeTo(9.1667, 0.0001));
    assertThat(first.getCount(), is(10L));
    assertThat(first.getMean(), closeTo(4.5, 0.000001));

    val merged = first.merge(new VarianceTracker());
    assertThat(merged.variance(), closeTo(9.1667, 0.0001));
    assertThat(merged.getCount(), is(10L));
    assertThat(merged.getMean(), closeTo(4.5, 0.000001));
  }

  @Test
  public void merge_EmptyTrackerMerging_ShouldBeCorrect() {
    val first = new VarianceTracker();

    val second = new VarianceTracker();
    // add a series of values [0,1,...,9]
    for (int i = 0; i < 10; i++) {
      second.update(i);
    }
    assertThat(second.variance(), closeTo(9.1667, 0.0001));
    assertThat(second.getCount(), is(10L));
    assertThat(second.getMean(), closeTo(4.5, 0.000001));

    val merged = first.merge(second);
    assertThat(merged.variance(), closeTo(9.1667, 0.0001));
    assertThat(merged.getCount(), is(10L));
    assertThat(merged.getMean(), closeTo(4.5, 0.000001));
  }

  @Test
  public void merge_BiggerTrackers_ShouldBeCorrect() {
    val first = new VarianceTracker();
    // add a series of values [0,1,...,9]
    for (int i = 0; i < 10; i++) {
      first.update(i);
    }
    assertThat(first.variance(), closeTo(9.1667, 0.0001));
    assertThat(first.getCount(), is(10L));
    assertThat(first.getMean(), closeTo(4.5, 0.000001));

    val merged = first.merge(first.copy());

    assertThat(merged.variance(), closeTo(8.684, 0.001));
    assertThat(merged.getCount(), is(20L));
    assertThat(merged.getMean(), closeTo(4.5, 0.000001));
  }

  @Test
  public void protobuf_RoundTrip_ShouldMatch() {
    val original = new VarianceTracker();
    // add a series of values [0,1,...,9]
    for (int i = 0; i < 10; i++) {
      original.update(i);
    }

    val proto = original.toProtobuf().build();
    final VarianceTracker roundtrip = VarianceTracker.fromProtobuf(proto);

    assertThat(original.getCount(), is(roundtrip.getCount()));
    assertThat(original.getMean(), is(roundtrip.getMean()));
    assertThat(original.getSum(), is(roundtrip.getSum()));
  }
}
