package com.whylogs.core.metrics;

import com.google.common.base.Preconditions;
import com.whylogs.core.message.RegressionMetricsMessage;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@RequiredArgsConstructor
@Getter
@Slf4j
public class RegressionMetrics {
  @NonNull private final String predictionField;
  @NonNull private final String targetField;
  private double sumAbsDiff;
  private double sumDiff;
  private double sum2Diff;
  private long count;

  public void track(Map<String, ?> columns) {
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
    res.count = this.count;
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
    return RegressionMetricsMessage.newBuilder()
        .setPredictionField(this.predictionField)
        .setTargetField(this.targetField)
        .setSumAbsDiff(this.sumAbsDiff)
        .setSumDiff(this.sumDiff)
        .setSum2Diff(this.sum2Diff)
        .setCount(this.count);
  }

  public static RegressionMetrics fromProtobuf(RegressionMetricsMessage msg) {
    if (msg == null || msg.getSerializedSize() == 0) {
      return null;
    }

    if ("".equals(msg.getPredictionField()) || "".equals(msg.getTargetField())) {
      log.warn("Skipping Regression metrics: prediction or target field not set");
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
