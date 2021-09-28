package com.whylogs.core.statistics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import lombok.val;
import org.testng.annotations.Test;

public class CountersTrackerTest {
  @Test
  public void counters_SimpleTracking() {
    val original = new CountersTracker();
    assertThat(original.getCount(), is(0L));

    original.incrementCount();
    original.incrementCount();

    assertThat(original.getCount(), is(2L));
    assertThat(original.getTrueCount(), is(0L));

    original.incrementTrue();
    assertThat(original.getTrueCount(), is(1L));

    val roundtrip = CountersTracker.fromProtobuf(original.toProtobuf().build());
    assertThat(roundtrip.getTrueCount(), is(1L));
    assertThat(roundtrip.getCount(), is(2L));
  }

  @Test
  public void merge_TwoTrackers_ShouldAddUp() {
    val first = new CountersTracker();
    first.incrementCount();
    first.incrementCount();
    first.incrementTrue();

    assertThat(first.getCount(), is(2L));
    assertThat(first.getTrueCount(), is(1L));

    val second = new CountersTracker();
    second.incrementCount();

    val merge1 = first.merge(second);

    assertThat(merge1.getCount(), is(3L));
    assertThat(merge1.getTrueCount(), is(1L));

    // should be associative
    val merge2 = second.merge(first);
    assertThat(merge2, equalTo(merge1));
  }
}
