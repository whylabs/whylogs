package com.whylabs.logging.core.statistics.datatypes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.testng.Assert.assertEquals;

import lombok.val;
import org.testng.annotations.Test;

public class DoubleTrackerTest {

  @Test
  public void track_DoubleValues_ShouldReflectMinAndMax() {
    val tracker = new DoubleTracker();
    tracker.update(1.0);
    tracker.update(2.0);
    tracker.update(3.0);

    assertEquals(tracker.getCount(), 3);
    assertEquals(tracker.getMin(), 1.0);
    assertEquals(tracker.getMax(), 3.0);
    assertEquals(tracker.getMean(), 2.0);
  }

  @Test
  public void merge_DoubleTrackers_ShouldAddUp() {
    val first = new DoubleTracker();
    first.update(1.0);
    first.update(2.0);
    first.update(3.0);

    assertThat(first.getCount(), is(3L));
    assertThat(first.getMax(), is(3.0));
    assertThat(first.getMin(), is(1.0));
    assertThat(first.getSum(), is(6.0));

    val second = new DoubleTracker();
    second.update(4.0);
    second.update(5.0);
    second.update(6.0);

    assertThat(second.getCount(), is(3L));
    assertThat(second.getMax(), is(6.0));
    assertThat(second.getMin(), is(4.0));
    assertThat(second.getSum(), is(15.0));

    val merge1st = first.merge(second);
    assertThat(merge1st.getCount(), is(6L));
    assertThat(merge1st.getMax(), is(6.0));
    assertThat(merge1st.getMin(), is(1.0));
    assertThat(merge1st.getSum(), is(21.0));

    val merge2nd = second.merge(first);
    assertThat(merge1st, is(merge2nd));
  }

  @Test
  public void protobuf_Serialization_ShouldWork() {
    val original = new DoubleTracker();
    original.update(1.0);
    original.update(2.0);
    original.update(3.0);

    val msg = original.toProtobuf().build();
    val roundTrip = DoubleTracker.fromProtobuf(msg);
    assertThat(roundTrip, is(original));
  }
}
