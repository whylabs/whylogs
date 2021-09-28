package com.whylogs.core.statistics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.whylogs.core.message.InferredType.Type;
import lombok.val;
import org.testng.annotations.Test;

public class SchemaTrackerTest {
  @Test
  public void track_Nothing_ShouldReturnUnknown() {
    val tracker = new SchemaTracker();

    val inferredType = tracker.getInferredType();
    assertThat(inferredType.getType(), is(Type.UNKNOWN));
    assertThat(inferredType.getRatio(), is(0.0));
  }

  @Test
  public void track_VariousDataTypes_ShouldHaveCorrectCount() {
    val tracker = new SchemaTracker();

    trackAFewTimes(tracker, Type.INTEGRAL, 2);
    assertThat(tracker.getCount(Type.INTEGRAL), is(2L));

    trackAFewTimes(tracker, Type.STRING, 2);
    assertThat(tracker.getCount(Type.STRING), is(2L));

    trackAFewTimes(tracker, Type.FRACTIONAL, 1);
    assertThat(tracker.getCount(Type.FRACTIONAL), is(1L));

    trackAFewTimes(tracker, Type.BOOLEAN, 2);
    assertThat(tracker.getCount(Type.BOOLEAN), is(2L));

    trackAFewTimes(tracker, Type.UNKNOWN, 2);
    assertThat(tracker.getCount(Type.UNKNOWN), is(2L));
  }

  @Test
  public void track_Over70PercentStringData_ShouldInferStringType() {
    val tracker = new SchemaTracker();

    trackAFewTimes(tracker, Type.INTEGRAL, 29);
    trackAFewTimes(tracker, Type.STRING, 71); // 71%

    val inferredType = tracker.getInferredType();
    assertThat(inferredType.getType(), is(Type.STRING));
  }

  @Test
  public void track_MajorityDoubleData_ShouldInferFractionalType() {
    val tracker = new SchemaTracker();

    trackAFewTimes(tracker, Type.FRACTIONAL, 51);
    trackAFewTimes(tracker, Type.STRING, 30);
    trackAFewTimes(tracker, Type.UNKNOWN, 20);

    val inferredType = tracker.getInferredType();
    assertThat(inferredType.getType(), is(Type.FRACTIONAL));
  }

  @Test
  public void track_HalfIsFractionalData_CannotInferType() {
    val tracker = new SchemaTracker();

    trackAFewTimes(tracker, Type.FRACTIONAL, 50);
    trackAFewTimes(tracker, Type.STRING, 30);
    trackAFewTimes(tracker, Type.UNKNOWN, 20);

    val inferredType = tracker.getInferredType();
    assertThat(inferredType.getType(), is(Type.UNKNOWN));
  }

  @Test
  public void track_MajorityIntegerAndLongData_ShouldInferIntegralType() {
    val tracker = new SchemaTracker();

    trackAFewTimes(tracker, Type.INTEGRAL, 51);
    trackAFewTimes(tracker, Type.STRING, 30);
    trackAFewTimes(tracker, Type.UNKNOWN, 20);

    assertThat(tracker.getInferredType().getType(), is(Type.INTEGRAL));
  }

  @Test
  public void track_DoubleAndLong_CoercedToFractional() {
    val tracker = new SchemaTracker();

    trackAFewTimes(tracker, Type.INTEGRAL, 50);
    trackAFewTimes(tracker, Type.FRACTIONAL, 50);
    trackAFewTimes(tracker, Type.STRING, 10);

    val inferredType = tracker.getInferredType();
    assertThat(inferredType.getType(), is(Type.FRACTIONAL));
  }

  @Test
  public void track_AllTypesEqual_CoercedToString() {
    val tracker = new SchemaTracker();

    trackAFewTimes(tracker, Type.INTEGRAL, 20);
    trackAFewTimes(tracker, Type.FRACTIONAL, 29);
    trackAFewTimes(tracker, Type.STRING, 50);

    val inferredType = tracker.getInferredType();

    assertThat(inferredType.getType(), is(Type.STRING));
  }

  @Test
  public void serialization_RoundTrip_ShouldMatch() {
    val tracker = new SchemaTracker();
    trackAFewTimes(tracker, Type.INTEGRAL, 10);
    trackAFewTimes(tracker, Type.STRING, 100);

    val protoBuf = tracker.toProtobuf();
    val roundtrip = SchemaTracker.fromProtobuf(protoBuf.build());

    assertThat(protoBuf.build(), is(roundtrip.toProtobuf().build()));
    assertThat(roundtrip.getCount(Type.INTEGRAL), is(10L));
    assertThat(roundtrip.getCount(Type.STRING), is(100L));
  }

  @Test
  public void merge_TotalCounts_ShouldMatch() {
    val first = new SchemaTracker();
    trackAFewTimes(first, Type.INTEGRAL, 10);
    trackAFewTimes(first, Type.FRACTIONAL, 10);
    trackAFewTimes(first, Type.BOOLEAN, 10);
    trackAFewTimes(first, Type.UNKNOWN, 10);

    val second = new SchemaTracker();
    trackAFewTimes(first, Type.INTEGRAL, 20);
    trackAFewTimes(first, Type.FRACTIONAL, 20);
    trackAFewTimes(first, Type.BOOLEAN, 20);
    trackAFewTimes(first, Type.UNKNOWN, 20);

    val merged = first.merge(second);
    assertThat(merged.getCount(Type.INTEGRAL), is(30L));
    assertThat(merged.getCount(Type.FRACTIONAL), is(30L));
    assertThat(merged.getCount(Type.BOOLEAN), is(30L));
    assertThat(merged.getCount(Type.UNKNOWN), is(30L));

    // should never contain this type. We can't serialize if this is the case
    assertThat(merged.getTypeCounts(), not(hasKey(Type.UNRECOGNIZED)));

    // verify troundtrip serialization
    SchemaTracker.fromProtobuf(merged.toProtobuf().build());
  }

  private static void trackAFewTimes(SchemaTracker original, Type type, int nTimes) {
    for (int i = 0; i < nTimes; i++) {
      original.track(type);
    }
  }
}
