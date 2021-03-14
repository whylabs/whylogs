package com.whylogs.core.metrics;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.whylogs.core.DatasetProfile;
import com.whylogs.core.message.ClassificationMetricsMessage;
import com.whylogs.core.statistics.NumberTracker;
import java.util.Collections;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ClassificationMetrics {
  private List<String> labels;
  private NumberTracker[][] values;

  public ClassificationMetrics() {
    this(Lists.newArrayList(), newMatrix(0));
  }

  public static ClassificationMetrics of() {
    return new ClassificationMetrics();
  }

  public List<String> getLabels() {
    return Collections.unmodifiableList(labels);
  }

  public long[][] getConfusionMatrix() {
    final int len = labels.size();
    val res = new long[len][len];
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        res[i][j] = values[i][j].getDoubles().getCount();
      }
    }
    return res;
  }

  private static NumberTracker[][] newMatrix(int len) {
    val res = new NumberTracker[len][len];
    if (len == 0) {
      return res;
    }
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        res[i][j] = new NumberTracker();
      }
    }

    return res;
  }

  public <T> void update(DatasetProfile datasetProfile, T prediction, T target, double score) {
    val predictionText = textValue(prediction);
    val targetText = textValue(target);
    datasetProfile.track("whylogs.metrics.predictions", predictionText);
    datasetProfile.track("whylogs.metrics.targets", targetText);

    val x = labels.indexOf(predictionText);
    val y = labels.indexOf(targetText);
    if (x >= 0 && y >= 0) {
      // happy case
      values[x][y].track(score);
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
      val newValues = newMatrix(newDim);

      // first copy existing values to the new matrix
      addMatrix(labels, values, newLabels, newValues);

      val i = newLabels.indexOf(predictionText);
      val j = newLabels.indexOf(targetText);
      newValues[i][j].track(score);

      this.labels = newLabels;
      this.values = newValues;
    }
  }

  private static String textValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean) {
      val boolVal = (Boolean) value;
      return boolVal ? "1" : "0";
    }
    return value.toString();
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

    val newValues = newMatrix(newLabels.size());

    // copy the current object
    addMatrix(labels, values, newLabels, newValues);

    // copy the other object
    addMatrix(other.labels, other.values, newLabels, newValues);

    return new ClassificationMetrics(newLabels, newValues);
  }

  private void addMatrix(
      List<String> oldLabels,
      NumberTracker[][] oldValues,
      List<String> newLabels,
      NumberTracker[][] newValues) {
    for (int i = 0; i < oldLabels.size(); i++) {
      val iLabel = oldLabels.get(i);
      final int newI = newLabels.indexOf(iLabel);
      for (int j = 0; j < oldLabels.size(); j++) {
        val jLabel = oldLabels.get(j);
        int newJ = newLabels.indexOf(jLabel);
        newValues[newI][newJ] = newValues[newI][newJ].merge(oldValues[i][j]);
      }
    }
  }

  @NonNull
  public ClassificationMetrics copy() {
    final int len = this.labels.size();
    val copyValues = newMatrix(len);
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        copyValues[i][j] = copyValues[i][j].merge(values[i][j]);
      }
    }
    return new ClassificationMetrics(Lists.newArrayList(this.labels), copyValues);
  }

  @NonNull
  @SuppressWarnings("UnstableApiUsage")
  public ClassificationMetricsMessage.Builder toProtobuf() {
    val builder = ClassificationMetricsMessage.newBuilder();
    labels.stream().map(Object::toString).forEach(builder::addLabels);

    val len = labels.size();
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        builder.addScores(values[i][j].toProtobuf());
      }
    }

    return builder;
  }

  public static ClassificationMetrics fromProtobuf(ClassificationMetricsMessage msg) {
    val labels = Lists.<String>newArrayList();
    for (int i = 0; i < msg.getLabelsCount(); i++) {
      labels.add(msg.getLabels(i));
    }

    final int n = labels.size();
    val values = newMatrix(n);
    for (int i = 0; i < msg.getScoresCount(); i++) {
      int row = i / n;
      int col = i % n;
      values[row][col] = NumberTracker.fromProtobuf(msg.getScores(i));
    }

    return new ClassificationMetrics(labels, values);
  }
}
