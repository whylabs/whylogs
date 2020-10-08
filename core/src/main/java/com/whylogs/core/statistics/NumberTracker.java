package com.whylogs.core.statistics;

import com.google.protobuf.ByteString;
import com.whylogs.core.message.NumbersMessage;
import com.whylogs.core.statistics.datatypes.DoubleTracker;
import com.whylogs.core.statistics.datatypes.LongTracker;
import com.whylogs.core.statistics.datatypes.VarianceTracker;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;

@Getter
@Builder(setterPrefix = "set")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class NumberTracker {
  // our own trackers
  VarianceTracker variance;
  DoubleTracker doubles;
  LongTracker longs;

  // sketches
  KllFloatsSketch histogram; // histogram

  public NumberTracker() {
    this.variance = new VarianceTracker();
    this.doubles = new DoubleTracker();
    this.longs = new LongTracker();

    this.histogram = new KllFloatsSketch(256);
  }

  public void track(Number number) {
    float dValue = number.floatValue();
    variance.update(dValue);
    histogram.update(dValue);

    if (doubles.getCount() > 0) {
      doubles.update(dValue);
    } else if (number instanceof Long || number instanceof Integer) {
      longs.update(number.longValue());
    } else {
      doubles.addLongs(longs);
      longs.reset();
      doubles.update(dValue);
    }
  }

  public void add(NumberTracker other) {
    if (other == null) {
      return;
    }

    this.variance.add(other.variance);
    this.doubles.add(other.doubles);
    this.longs.add(other.longs);
    this.histogram.merge(other.histogram);
  }

  public NumberTracker merge(NumberTracker other) {
    if (other == null) {
      return this;
    }

    val unionHistogram = KllFloatsSketch.heapify(Memory.wrap(this.histogram.toByteArray()));
    unionHistogram.merge(other.histogram);

    return NumberTracker.builder()
        .setVariance(this.variance.merge(other.variance))
        .setDoubles(this.doubles.merge(other.doubles))
        .setLongs(this.longs.merge(other.longs))
        .setHistogram(unionHistogram)
        .build();
  }

  public NumbersMessage.Builder toProtobuf() {
    val builder =
        NumbersMessage.newBuilder()
            .setVariance(variance.toProtobuf())
            .setHistogram(ByteString.copyFrom(histogram.toByteArray()));

    if (this.doubles.getCount() > 0) {
      builder.setDoubles(this.doubles.toProtobuf());
    } else if (this.longs.getCount() > 0) {
      builder.setLongs(this.longs.toProtobuf());
    }

    return builder;
  }

  public static NumberTracker fromProtobuf(NumbersMessage message) {
    val hMem = Memory.wrap(message.getHistogram().toByteArray());
    val builder =
        NumberTracker.builder()
            .setHistogram(KllFloatsSketch.heapify(hMem))
            .setVariance(VarianceTracker.fromProtobuf(message.getVariance()));

    Optional.ofNullable(message.getDoubles())
        .map(DoubleTracker::fromProtobuf)
        .ifPresent(builder::setDoubles);
    Optional.ofNullable(message.getLongs())
        .map(LongTracker::fromProtobuf)
        .ifPresent(builder::setLongs);

    return builder.build();
  }
}
