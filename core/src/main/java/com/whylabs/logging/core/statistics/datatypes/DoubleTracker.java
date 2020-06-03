package com.whylabs.logging.core.statistics.datatypes;

import com.whylabs.logging.core.format.DoublesMessage;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.val;

@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DoubleTracker {

  private double min;
  private double max;
  private double sum;
  private long count;

  public DoubleTracker() {
    this.min = Double.MAX_VALUE;
    this.max = -Double.MAX_VALUE;
    this.sum = 0;
    this.count = 0;
  }

  public void addLongs(LongTracker longs) {
    if (longs != null && longs.getCount() != 0) {
      this.min = longs.getMin();
      this.max = longs.getMax();
      this.sum = longs.getSum();
      this.count = longs.getCount();
    }
  }

  public double getMean() {
    return sum / count;
  }

  public void update(double value) {
    if (value > max) {
      max = value;
    }
    if (value < min) {
      min = value;
    }
    count++;
    sum += value;
  }

  public DoubleTracker merge(DoubleTracker other) {
    val thisCopy = new DoubleTracker(min, max, sum, count);
    if (other.min < thisCopy.min) {
      thisCopy.min = other.min;
    }

    if (other.max > thisCopy.max) {
      thisCopy.max = other.max;
    }
    thisCopy.sum += other.sum;
    thisCopy.count += other.count;

    return thisCopy;
  }

  public DoublesMessage.Builder toProtobuf() {
    return DoublesMessage.newBuilder().setCount(count).setSum(sum).setMin(min).setMax(max);
  }

  public static DoubleTracker fromProtobuf(DoublesMessage message) {
    val tracker = new DoubleTracker();
    tracker.count = message.getCount();
    tracker.max = message.getMax();
    tracker.min = message.getMin();
    tracker.sum = message.getSum();
    return tracker;
  }
}
