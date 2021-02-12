package com.whylogs.core.metrics;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.whylogs.core.DatasetProfile;
import com.whylogs.core.message.ClassificationMetricsMessage;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ClassificationMetrics {
  @Getter
  private List<String> labels = Lists.newArrayList();
  private long[][] values = new long[0][0];

  public ClassificationMetrics() {
    this(Lists.newArrayList(), new long[0][0]);
  }

  public static ClassificationMetrics of() {
    return new ClassificationMetrics();
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
    val predictionText = prediction.toString();
    val targetText = target.toString();
    datasetProfile.track("whylogs.metrics.predictions", predictionText);
    datasetProfile.track("whylogs.metrics.targets", targetText);

    val x = labels.indexOf(predictionText);
    val y = labels.indexOf(target.toString());
    if (x >= 0 && y >= 0) {
      // happy case
      values[x][y]++;

      val correctColumn = "whylogs.metrics.scores.correct." + predictionText;
      val incorrectColumn = "whylogs.metrics.scores.incorrect." + predictionText;
      if (prediction.equals(target)) {
        datasetProfile.track(correctColumn, score);
      } else {
        datasetProfile.track(incorrectColumn, score);
      }
    } else {
      val newLabels = Lists.newArrayList(labels);

      if (x < 0) {
        newLabels.add(predictionText);
      }
      if (y < 0) {
        newLabels.add(targetText);
      }
      Collections.sort(newLabels);

      final int newDim = newLabels.size();
      val newValues = new long[newDim][newDim];

      // first copy existing values to the new matrix
      addMatrix(labels, values, newLabels, newValues);

      val i = newLabels.indexOf(predictionText);
      val j = newLabels.indexOf(targetText);
      newValues[i][j] += 1;

      this.labels = newLabels;
      this.values = newValues;
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

    val allLabels = Sets.newHashSet(this.labels);
    allLabels.addAll(other.labels);
    val newLabels = Lists.newArrayList(allLabels);
    Collections.sort(newLabels);

    final int newLen = newLabels.size();
    val newValues = new long[newLen][newLen];

    // copy the current object
    addMatrix(labels, values, newLabels, newValues);

    // copy the other object
    addMatrix(other.labels, other.values, newLabels, newValues);

    return new ClassificationMetrics(newLabels, newValues);
  }

  private void addMatrix(List<String> oldLabels, long[][] oldValues, List<String> newLabels, long[][] newValues) {
    for (int i = 0; i < oldLabels.size(); i++) {
      val iLabel = oldLabels.get(i);
      final int newI = newLabels.indexOf(iLabel);
      for (int j = 0; j < oldLabels.size(); j++) {
        val jLabel = oldLabels.get(j);
        int newJ = newLabels.indexOf(jLabel);
        newValues[newI][newJ] += oldValues[i][j];
      }
    }
  }

  @NonNull
  public ClassificationMetrics copy() {
    return new ClassificationMetrics(Lists.newArrayList(this.labels), this.getConfusionMatrix());
  }

  @NonNull
  @SuppressWarnings("UnstableApiUsage")
  public ClassificationMetricsMessage toProtobuf() {
    val builder =
        ClassificationMetricsMessage.newBuilder();
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

    return new ClassificationMetrics(labels, values);
  }
}
