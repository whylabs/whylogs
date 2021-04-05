package com.whylogs.core.metrics;

import com.google.common.base.Preconditions;
import com.whylogs.core.message.RegressionMetricsMessage;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
@Getter
public class RegressionMetrics {
  private final String predictionField;
  private final String targetField;
  private double sumAbsDiff;
  private double sumDiff;
  private double sum2Diff;
  private long count;

  public void track(Map<String, ?> columns) {
    Preconditions.checkState(predictionField != null);
    Preconditions.checkState(targetField != null);
    val prediction = (Number) columns.get(predictionField);
    val target = (Number) columns.get(targetField);

    final double diff = prediction.doubleValue() - target.doubleValue();
    this.sumAbsDiff += Math.abs(diff);
    this.sumDiff += diff;
    this.sum2Diff += diff * diff;
    this.count++;
  }

  public RegressionMetrics copy() {
    val res = new RegressionMetrics(this.predictionField, this.targetField);
    res.sumAbsDiff = this.sumAbsDiff;
    res.sumDiff = this.sumDiff;
    res.sum2Diff = this.sum2Diff;
    return res;
  }

  public RegressionMetrics merge(RegressionMetrics other) {
    if (other == null) {
      return this.copy();
    }

    Preconditions.checkState(
        Objects.equals(this.predictionField, other.predictionField),
        "Mismatched prediction fields: %s vs %s",
        this.predictionField,
        other.predictionField);
    Preconditions.checkState(
        Objects.equals(this.targetField, other.targetField),
        "Mismatched target fields: %s vs %s",
        this.targetField,
        other.targetField);

    val result = new RegressionMetrics(this.predictionField, this.targetField);
    result.sumAbsDiff = this.sumAbsDiff + other.sumAbsDiff;
    result.sumDiff = this.sumDiff + other.sumDiff;
    result.sum2Diff = this.sum2Diff + other.sum2Diff;
    result.count = this.count + other.count;

    return result;
  }

  public RegressionMetricsMessage.Builder toProtobuf() {
    if (this.predictionField == null | this.targetField == null) {
      return null;
    }
    return RegressionMetricsMessage.newBuilder()
        .setPredictionField(this.predictionField)
        .setTargetField(this.targetField)
        .setSumAbsDiff(this.sumAbsDiff)
        .setSumDiff(this.sumDiff)
        .setSum2Diff(this.sum2Diff)
        .setCount(this.count);
  }

  public static RegressionMetrics fromProtobuf(RegressionMetricsMessage msg) {
    if (msg == null) {
      return null;
    }

    if ("".equals(msg.getPredictionField()) || "".equals(msg.getTargetField())) {
      return null;
    }

    val res = new RegressionMetrics(msg.getPredictionField(), msg.getTargetField());
    res.sumAbsDiff = msg.getSumAbsDiff();
    res.sumDiff = msg.getSumDiff();
    res.sum2Diff = msg.getSum2Diff();
    res.count = msg.getCount();
    return res;
  }
}
