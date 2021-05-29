package com.whylogs.core.statistics;

import com.google.protobuf.Int64Value;
import com.whylogs.core.message.Counters;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.val;

@EqualsAndHashCode
@Getter
@FieldDefaults(level = AccessLevel.PRIVATE)
public class CountersTracker {
  long count;
  long trueCount;

  public void incrementCount() {
    count++;
  }

  public void incrementTrue() {
    trueCount++;
  }

  public void add(CountersTracker other) {
    this.count += other.count;
    this.trueCount += other.trueCount;
  }

  public CountersTracker merge(CountersTracker other) {
    val result = new CountersTracker();
    result.count = this.count + other.count;
    result.trueCount = this.trueCount + other.trueCount;

    return result;
  }

  public Counters.Builder toProtobuf() {
    val countersBuilder = Counters.newBuilder().setCount(count);

    if (trueCount > 0) {
      countersBuilder.setTrueCount(Int64Value.of(trueCount));
    }

    return countersBuilder;
  }

  public static CountersTracker fromProtobuf(Counters message) {
    val tracker = new CountersTracker();
    tracker.count = message.getCount();
    tracker.trueCount = Optional.of(message.getTrueCount()).map(Int64Value::getValue).orElse(0L);

    return tracker;
  }
}
