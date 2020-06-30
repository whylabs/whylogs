package com.whylabs.logging.core.statistics.datatypes;

import com.whylabs.logging.core.message.VarianceMessage;
import lombok.Getter;
import lombok.val;

@Getter
public class VarianceTracker {
  long count;
  double sum; // sample variance * (n-1)
  double mean;

  public VarianceTracker() {
    this.count = 0L;
    this.sum = 0L;
    this.mean = 0L;
  }

  // Based on
  // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
  public void update(double newValue) {
    count++;

    double delta = newValue - mean;
    mean += delta / count;
    double delta2 = newValue - mean;
    sum += delta * delta2;
  }

  /** @return sample standard deviation */
  public double stddev() {
    return Math.sqrt(this.variance());
  }

  /** @return the sample variance */
  public double variance() {
    if (count < 2) {
      return Double.NaN;
    }

    return sum / (count - 1.0);
  }

  /** https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm */
  public VarianceTracker merge(VarianceTracker other) {
    final VarianceTracker thisCopy = this.copy();
    if (other.count == 0L) {
      return thisCopy;
    }

    if (this.count == 0L) {
      return other.copy();
    }

    val delta = thisCopy.mean - other.mean;
    val totalCount = thisCopy.count + other.count;
    thisCopy.sum +=
        other.sum + Math.pow(delta, 2) * thisCopy.count * other.count / (double) totalCount;

    val thisRatio = thisCopy.count / (double) totalCount;
    val otherRatio = 1.0 - thisRatio;
    thisCopy.mean = thisCopy.mean * thisRatio + other.mean * otherRatio;
    thisCopy.count += other.count;

    return thisCopy;
  }

  VarianceTracker copy() {
    val result = new VarianceTracker();
    result.count = this.count;
    result.sum = this.sum;
    result.mean = this.mean;
    return result;
  }

  public VarianceMessage.Builder toProtobuf() {
    return VarianceMessage.newBuilder().setCount(count).setMean(mean).setSum(sum);
  }

  public static VarianceTracker fromProtobuf(VarianceMessage message) {
    final VarianceTracker tracker = new VarianceTracker();
    tracker.count = message.getCount();
    tracker.mean = message.getMean();
    tracker.sum = message.getSum();
    return tracker;
  }
}
