package com.whylogs.core.metrics;

import com.google.common.base.Preconditions;
import com.whylogs.core.message.ModelMetricsMessage;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.Map;

@RequiredArgsConstructor
public class ModelMetrics {
  @Getter
  private final ScoreMatrix scoreMatrix;

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
    if (msg == null || msg.getSerializedSize() == 0) {
      return null;
    }

    val scoreMatrix = ScoreMatrix.fromProtobuf(msg.getScoreMatrix());
    if (scoreMatrix == null) {
      return null;
    }
    return new ModelMetrics(scoreMatrix);
  }

  public void track(Map<String, ?> columns) {
    this.scoreMatrix.track(columns);
  }
}
