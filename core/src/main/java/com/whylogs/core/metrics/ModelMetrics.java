package com.whylogs.core.metrics;

import com.google.common.base.Preconditions;
import com.whylogs.core.message.ModelMetricsMessage;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class ModelMetrics {
  @Getter private final ScoreMatrix scoreMatrix;

  public ModelMetrics(String predictionField, String targetField, String scoreField) {
    this(new ScoreMatrix(predictionField, targetField, scoreField));
  }

  public <T> void trackScore(T prediction, T target, double score) {
    Preconditions.checkState(scoreMatrix != null);
    this.scoreMatrix.update(prediction, target, score);
  }

  public ModelMetricsMessage.Builder toProtobuf() {
    val res = ModelMetricsMessage.newBuilder();
    if (scoreMatrix != null) {
      res.setScoreMatrix(scoreMatrix.toProtobuf());
    }
    return res;
  }

  public ModelMetrics merge(ModelMetrics metrics) {
    val mergedScoreMatrix = this.scoreMatrix.merge(metrics.scoreMatrix);
    return new ModelMetrics(mergedScoreMatrix);
  }

  public ModelMetrics copy() {
    return new ModelMetrics(scoreMatrix.copy());
  }

  public static ModelMetrics fromProtobuf(ModelMetricsMessage msg) {
    if (msg == null) {
      return null;
    }
    val scoreMatrix = msg.getScoreMatrix();
    if (scoreMatrix == null) {
      return new ModelMetrics(null);
    }

    return new ModelMetrics(ScoreMatrix.fromProtobuf(scoreMatrix));
  }

  public void track(Map<String, ?> columns) {
    this.scoreMatrix.track(columns);
  }
}
