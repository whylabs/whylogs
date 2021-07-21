package com.whylogs.core.statistics.datatypes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.val;
import org.testng.annotations.Test;

public class StringTrackerTest {
  @Test
  public void stringTracker_characterPosTracking() {
    /* based on python test_character_pos_tracker() */

    val tracker = new StringTracker();
    val data = Arrays.asList("abc abc", "93341-1", "912254", null);
    val noNulls = data.stream().filter(Objects::nonNull).collect(Collectors.toList());
    val count = noNulls.size();
    val n_unique = ImmutableSet.copyOf(noNulls).size();

    data.forEach(tracker::update);

    assertThat(tracker.getItems().getNumActiveItems(), is(n_unique));
    assertThat(tracker.getItems().getStreamLength(), is((long) count));

    assertEquals(tracker.getLength().getLongs().getMean(), 6.666, 0.001);
    assertEquals(tracker.getTokenLength().getLongs().getMean(), 1.333, 0.001);
    assertEquals(tracker.getThetaSketch().getResult().getEstimate(), n_unique, 0.0);

    assertThat(tracker.getCharPosTracker(), is(notNullValue()));
    assertThat(tracker.getCount(), is((long) count));
    assertThat(
        tracker.getCharPosTracker().getCharPosMap().get('a').getHistogram().getMinValue(),
        is(0.0F));
    assertThat(
        tracker.getCharPosTracker().getCharPosMap().get('a').getLongs().getCount(), is((long) 2));
    assertThat(
        tracker.getCharPosTracker().getCharPosMap().get('-').getHistogram().getMinValue(),
        is(5.0F));
  }

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
    val x = new StringTracker();
    val y = new StringTracker();
    val data = Arrays.asList("abc abc", "93341-1", "912254", "bac tralalala");
    val data2 =
        Arrays.asList(
            "geometric inference ",
            "93341-1",
            "912254",
            "bac tralalala",
            "😀 this is a sale! a ❄️ sale!",
            "this is a long sentence that ends in an A",
            null);
    data.forEach(x::update);
    data2.forEach(y::update);

    assertThat(x.getCharPosTracker().getCharPosMap().get((char) 0).getLongs().getCount(), is(2L));
    assertThat(
        x.getCharPosTracker().getCharPosMap().get('a').getHistogram().getMaxValue(), is(12F));
    assertThat(y.getCharPosTracker().getCharPosMap().get((char) 0).getLongs().getCount(), is(22L));
    assertThat(
        y.getCharPosTracker().getCharPosMap().get('a').getHistogram().getMaxValue(), is(40F));

    val z = x.merge(y);
    assertThat(
        z.getCharPosTracker().getCharPosMap().get('a').getHistogram().getMaxValue(), is(40F));
    assertThat(z.getCharPosTracker().getCharPosMap().get((char) 0).getLongs().getCount(), is(24L));

    assertThat(z.getCount(), is(10L));
    assertThat(z.getItems().getNumActiveItems(), is(7));

    // verify we can serialize the merge object
    val msg = z.toProtobuf().build();
    StringTracker.fromProtobuf(msg);
  }

  @Test
  public void stringTracker_MergeCharacterLists() {
    val x = new StringTracker();
    val y = new StringTracker();
    val data = Arrays.asList("abc abc", "93341-1", "912254", "bac tralalala");
    val data2 =
        Arrays.asList(
            "geometric inference ",
            "93341-1",
            "912254",
            "bac tralalala",
            "😀 this is a sale! a ❄️ sale!",
            "this is a long sentence that ends in an A",
            null);
    data.forEach(d -> x.update(d, "ab"));
    data2.forEach(d -> y.update(d, "a"));
    assertThat(x.getCharPosTracker().getCharPosMap().get((char) 0).getLongs().getCount(), is(23L));
    assertThat(
        x.getCharPosTracker().getCharPosMap().get('a').getHistogram().getMaxValue(), is(12F));
    assertThat(y.getCharPosTracker().getCharPosMap().get((char) 0).getLongs().getCount(), is(102L));
    assertThat(
        y.getCharPosTracker().getCharPosMap().get('a').getHistogram().getMaxValue(), is(40F));

    val z = x.merge(y);
    assertThat(
        z.getCharPosTracker().getCharPosMap().get('a').getHistogram().getMaxValue(), is(40F));
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
