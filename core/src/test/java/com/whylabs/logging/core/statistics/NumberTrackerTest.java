package com.whylabs.logging.core.statistics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import lombok.val;
import org.testng.annotations.Test;

public class NumberTrackerTest {

  @Test
  public void track_LongValue_ShouldNotIncreaseDoubleCount() {
    val numberTracker = new NumberTracker();
    numberTracker.track(10L);
    numberTracker.track(11L);
    numberTracker.track(12);

    assertThat(numberTracker.getLongs().getCount(), is(3L));
    assertThat(numberTracker.getDoubles().getCount(), is(0L));
    assertThat(numberTracker.getVariance().stddev(), closeTo(1.0, 0.001));

    assertThat(numberTracker.getHistogram().getN(), is(3L));
    assertThat(numberTracker.getThetaSketch().getEstimate(), closeTo(3, 0.001));
    assertThat((double) numberTracker.getHistogram().getMaxValue(), closeTo(12, 0.0001));
    assertThat((double) numberTracker.getHistogram().getMinValue(), closeTo(10, 0.0001));
  }

  @Test
  public void track_DoubleValue_ShouldNotIncreaseLongCount() {
    val numberTracker = new NumberTracker();
    numberTracker.track(10.0);
    numberTracker.track(11.0);
    numberTracker.track(12.0);

    assertThat(numberTracker.getLongs().getCount(), is(0L));
    assertThat(numberTracker.getDoubles().getCount(), is(3L));
    assertThat(numberTracker.getVariance().stddev(), closeTo(1.0, 0.001));

    assertThat(numberTracker.getHistogram().getN(), is(3L));
    assertThat(numberTracker.getThetaSketch().getEstimate(), closeTo(3, 0.001));
    assertThat((double) numberTracker.getHistogram().getMaxValue(), closeTo(12, 0.0001));
    assertThat((double) numberTracker.getHistogram().getMinValue(), closeTo(10, 0.0001));
  }

  @Test
  public void track_DoubleValueAfterLongValue_ShouldResetLongsTracker() {
    val numberTracker = new NumberTracker();
    numberTracker.track(10L);
    numberTracker.track(11L);
    assertThat(numberTracker.getLongs().getCount(), is(2L));
    assertThat(numberTracker.getDoubles().getCount(), is(0L));

    // instead of Long, we got a double value here
    numberTracker.track(12.0);

    assertThat(numberTracker.getLongs().getCount(), is(0L));
    assertThat(numberTracker.getDoubles().getCount(), is(3L));
    assertThat(numberTracker.getVariance().stddev(), closeTo(1.0, 0.001));

    assertThat(numberTracker.getHistogram().getN(), is(3L));
    assertThat(numberTracker.getThetaSketch().getEstimate(), closeTo(3, 0.001));
    assertThat((double) numberTracker.getHistogram().getMaxValue(), closeTo(12, 0.0001));
    assertThat((double) numberTracker.getHistogram().getMinValue(), closeTo(10, 0.0001));
  }

  @Test
  public void merge_TwoNumberTrackers_ShouldSuccess() {
    val numberTracker = new NumberTracker();
    numberTracker.track(10L);
    numberTracker.track(11L);
    numberTracker.track(13L);

    assertThat(numberTracker.getLongs().getCount(), is(3L));
    assertThat(numberTracker.getDoubles().getCount(), is(0L));
    assertThat(numberTracker.getLongs().getCount(), is(3L));
    assertThat(numberTracker.getHistogram().getN(), is(3L));
    assertThat(numberTracker.getHistogram().getMaxValue(), is(13.0f));
    assertThat(numberTracker.getHistogram().getMinValue(), is(10.0f));

    val merged = numberTracker.merge(numberTracker);
    assertThat(merged.getLongs().getCount(), is(6L));
    assertThat(merged.getDoubles().getCount(), is(0L));
    assertThat(merged.getLongs().getCount(), is(6L));
    assertThat(merged.getHistogram().getN(), is(6L));
    assertThat(merged.getHistogram().getMaxValue(), is(13.0f));
    assertThat(merged.getHistogram().getMinValue(), is(10.0f));

    // test serialization with merged object
    val mergedMsg = merged.toProtobuf().build();
    NumberTracker.fromProtobuf(mergedMsg);
  }

  @Test
  public void serialization_NumberTracker_Roundtrip() {
    val original = new NumberTracker();
    original.track(10L);
    original.track(11L);
    original.track(13L);

    val msg = original.toProtobuf().build();
    val roundtrip = NumberTracker.fromProtobuf(msg);

    assertThat(roundtrip.getLongs().getCount(), is(3L));
    assertThat(roundtrip.getDoubles().getCount(), is(0L));
    assertThat(roundtrip.getLongs().getCount(), is(3L));
    assertThat(roundtrip.getHistogram().getN(), is(3L));
    assertThat(roundtrip.getHistogram().getMaxValue(), is(13.0f));
    assertThat(roundtrip.getHistogram().getMinValue(), is(10.0f));
  }
}
