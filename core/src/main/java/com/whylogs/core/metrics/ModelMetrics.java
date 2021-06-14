package com.whylogs.core.metrics;

import com.google.common.base.Preconditions;
import com.whylogs.core.message.ModelMetricsMessage;
import com.whylogs.core.message.ModelType;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ModelMetrics {
  @Getter private final ModelType modelType;
  @Getter private final ScoreMatrix scoreMatrix;
  @Getter private final RegressionMetrics regressionMetrics;

  public ModelMetrics(String predictionField, String targetField, String scoreField) {
    this(ModelType.CLASSIFICATION, new ScoreMatrix(predictionField, targetField, scoreField), null);
  }

  public ModelMetrics(String predictionField, String targetField) {
    this(ModelType.REGRESSION, null, new RegressionMetrics(predictionField, targetField));
  }

  public void track(Map<String, ?> columns) {
    switch (modelType) {
      case CLASSIFICATION:
        this.scoreMatrix.track(columns);
        break;
      case REGRESSION:
        this.regressionMetrics.track(columns);
        break;
      default:
        throw new IllegalArgumentException("Unsupported model type: " + modelType);
    }
  }

  public ModelMetricsMessage.Builder toProtobuf() {
    val res = ModelMetricsMessage.newBuilder().setModelType(this.modelType);
    if (scoreMatrix != null) {
      res.setScoreMatrix(scoreMatrix.toProtobuf());
    }
    if (regressionMetrics != null) {
      res.setRegressionMetrics(regressionMetrics.toProtobuf());
    }

    return res;
  }

  public ModelMetrics merge(ModelMetrics other) {
    if (other == null) {
      return this;
    }
    Preconditions.checkArgument(
        this.modelType == other.modelType,
        "Mismatched model type: expected %s, got %s",
        this.modelType,
        other.modelType);

    switch (this.modelType) {
      case CLASSIFICATION:
        val mergedMatrix = scoreMatrix.merge(other.scoreMatrix);
        return new ModelMetrics(this.modelType, mergedMatrix, null);
      case REGRESSION:
        val mergedRegressionMetrics = regressionMetrics.merge(other.regressionMetrics);
        return new ModelMetrics(this.modelType, null, mergedRegressionMetrics);
      default:
        throw new IllegalArgumentException("Unsupported model type: " + this.modelType);
    }
  }

  public ModelMetrics copy() {
    switch (this.modelType) {
      case CLASSIFICATION:
        return new ModelMetrics(this.modelType, this.scoreMatrix.copy(), null);
      case REGRESSION:
        return new ModelMetrics(this.modelType, null, this.regressionMetrics.copy());
      default:
        throw new IllegalArgumentException("Unsupported model type: " + this.modelType);
    }
  }

  public static ModelMetrics fromProtobuf(ModelMetricsMessage msg) {
    if (msg == null || msg.getSerializedSize() == 0) {
      return null;
    }
    val scoreMatrix = ScoreMatrix.fromProtobuf(msg.getScoreMatrix());
    val regressionMetrics = RegressionMetrics.fromProtobuf(msg.getRegressionMetrics());
    return new ModelMetrics(msg.getModelType(), scoreMatrix, regressionMetrics);
  }
}
