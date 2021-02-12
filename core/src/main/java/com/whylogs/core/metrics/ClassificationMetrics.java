package com.whylogs.core.metrics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.whylogs.core.DatasetProfile;
import com.whylogs.core.message.ClassificationMetricsMessage;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ClassificationMetrics {
  @Getter private final List<?> labels;
  private final long[][] values;
  @Getter private int unlabeledPredictionCount;
  @Getter private int unlabeledTargetCount;

  public static ClassificationMetrics of(List<?> labels) {
    final int len = labels.size();
    val values = new long[len][len];
    return new ClassificationMetrics(ImmutableList.copyOf(labels), values, 0, 0);
  }

  public long[][] getConfusionMatrix() {
    final int len = labels.size();
    val res = new long[len][len];
    for (int i = 0; i < len; i++) {
      System.arraycopy(values[i], 0, res[i], 0, len);
    }
    return res;
  }

  public <T> void update(DatasetProfile datasetProfile, T prediction, T target, double score) {
    datasetProfile.track("whylogs.metrics.predictions", prediction.toString());
    datasetProfile.track("whylogs.metrics.targets", prediction.toString());

    val x = labels.indexOf(prediction);
    val y = labels.indexOf(target);
    if (x >= 0 && y >= 0) {
      values[x][y]++;

      val correctColumn = "whylogs.metrics.scores.correct." + prediction.toString();
      val incorrectColumn = "whylogs.metrics.scores.incorrect." + prediction.toString();
      if (prediction.equals(target)) {
        datasetProfile.track(correctColumn, score);
      } else {
        datasetProfile.track(incorrectColumn, score);
      }
    }
    if (x < 0) {
      unlabeledPredictionCount += 1;
    }
    if (y < 0) {
      unlabeledTargetCount += 1;
    }
  }

  @Override
  public String toString() {
    val builder = new StringBuilder();
    builder.append("Labels: ");
    labels.forEach(
        it -> {
          builder.append(it);
          builder.append(", ");
        });
    labels.forEach(builder::append);
    builder.append('\n');

    final int len = labels.size();
    for (int i = 0; i < len; i++) {
      builder.append('[');
      for (int j = 0; j < len; j++) {
        builder.append(values[i][j]);
        if (j + 1 < len) {
          builder.append(", ");
        }
      }
      builder.append("]\n");
    }
    return builder.toString();
  }

  public ClassificationMetrics merge(ClassificationMetrics other) {
    if (other == null) {
      return copy();
    }

    Preconditions.checkArgument(
        this.labels.equals(other.labels), "Cannot merge confusion matrices with mismatched labels");

    int len = labels.size();
    val result = new long[len][len];
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        result[i][j] = this.values[i][j] + other.values[i][j];
      }
    }

    final int untrackedPredictionsSum =
        this.unlabeledPredictionCount + other.unlabeledPredictionCount;
    final int untrackedTargetsSum = this.unlabeledTargetCount + other.unlabeledTargetCount;

    return new ClassificationMetrics(labels, result, untrackedPredictionsSum, untrackedTargetsSum);
  }

  @NonNull
  public ClassificationMetrics copy() {
    return new ClassificationMetrics(
        labels, getConfusionMatrix(), unlabeledPredictionCount, unlabeledTargetCount);
  }

  @NonNull
  @SuppressWarnings("UnstableApiUsage")
  public ClassificationMetricsMessage toProtobuf() {
    val builder =
        ClassificationMetricsMessage.newBuilder()
            .setUnlabeledPredictionCount(unlabeledPredictionCount)
            .setUnlabeledTargetCount(unlabeledTargetCount);
    labels.stream().map(Object::toString).forEach(builder::addLabels);

    val len = labels.size();
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        builder.addConfusionMatrixValue(values[i][j]);
      }
    }

    return builder.build();
  }

  public static ClassificationMetrics fromProtobuf(ClassificationMetricsMessage msg) {
    val labels = Lists.<String>newArrayList();
    for (int i = 0; i < msg.getLabelsCount(); i++) {
      labels.add(msg.getLabels(i));
    }

    final int n = labels.size();
    val values = new long[n][n];
    for (int i = 0; i < msg.getConfusionMatrixValueCount(); i++) {
      int row = i / n;
      int col = i % n;
      values[row][col] = msg.getConfusionMatrixValue(i);
    }

    return new ClassificationMetrics(
        labels, values, msg.getUnlabeledPredictionCount(), msg.getUnlabeledTargetCount());
  }
}
