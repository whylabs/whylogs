package com.whylogs.core.statistics.datatypes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import lombok.val;
import org.hamcrest.Matchers;
import org.testng.annotations.Test;

public class LongTrackerTest {

  @Test
  public void update_BasicCase_ShouldBeCorrect() {
    val tracker = new LongTracker();
    tracker.update(1L);
    tracker.update(2L);
    tracker.update(3L);

    assertThat(tracker.getCount(), is(3L));
    assertThat(tracker.getMax(), is(3L));
    assertThat(tracker.getMin(), is(1L));
    assertThat(tracker.getMean(), is(2.0));
  }

  @Test
  public void merge_LongTrackers_ShouldAddUp() {
    val first = new LongTracker();
    first.update(1);
    first.update(2);
    first.update(3);

    assertThat(first.getCount(), is(3L));
    assertThat(first.getMax(), is(3L));
    assertThat(first.getMin(), is(1L));
    assertThat(first.getSum(), is(6L));

    val second = new LongTracker();
    second.update(4);
    second.update(5);
    second.update(6);

    assertThat(second.getCount(), is(3L));
    assertThat(second.getMax(), is(6L));
    assertThat(second.getMin(), is(4L));
    assertThat(second.getSum(), is(15L));

    val merge1st = first.merge(second);
    assertThat(merge1st.getCount(), is(6L));
    assertThat(merge1st.getMax(), is(6L));
    assertThat(merge1st.getMin(), is(1L));
    assertThat(merge1st.getSum(), is(21L));

    val merge2nd = second.merge(first);
    assertThat(merge1st, is(merge2nd));
  }

  @Test
  public void protobuf_Serialization_ShouldWork() {
    val original = new LongTracker();
    original.update(1L);
    original.update(2L);
    original.update(3L);

    val msg = original.toProtobuf().build();
    val roundTrip = LongTracker.fromProtobuf(msg);
    assertThat(roundTrip, Matchers.is(original));
  }
}
