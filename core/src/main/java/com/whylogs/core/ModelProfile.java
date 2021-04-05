package com.whylogs.core;

import com.google.common.collect.Sets;
import com.whylogs.core.message.ModelProfileMessage;
import com.whylogs.core.metrics.ModelMetrics;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ModelProfile {
  private final Set<String> outputFields;
  @Getter private final ModelMetrics metrics;

  public ModelProfile(
      String prediction, String target, String score, Iterable<String> additionalOutputFields) {
    this.outputFields = Sets.newHashSet(additionalOutputFields);
    this.outputFields.add(prediction);
    this.metrics = new ModelMetrics(prediction, target, score);
  }

  public ModelProfile(String prediction, String target, Iterable<String> additionalOutputFields) {
    this.outputFields = Sets.newHashSet(additionalOutputFields);
    this.outputFields.add(prediction);
    this.metrics = new ModelMetrics(prediction, target);
  }

  public ModelProfileMessage.Builder toProtobuf() {
    val builder = ModelProfileMessage.newBuilder();
    builder.addAllOutputFields(outputFields);
    if (metrics != null) {
      builder.setMetrics(metrics.toProtobuf());
    }
    return builder;
  }

  public static ModelProfile fromProtobuf(ModelProfileMessage message) {
    if (message == null || message.getSerializedSize() == 0) {
      return null;
    }

    val metrics = ModelMetrics.fromProtobuf(message.getMetrics());
    val outputFields = Sets.newHashSet(message.getOutputFieldsList());

    return new ModelProfile(outputFields, metrics);
  }

  public ModelProfile merge(ModelProfile other) {
    if (other == null) {
      return new ModelProfile(Sets.newHashSet(outputFields), metrics.copy());
    }
    return new ModelProfile(Sets.newHashSet(outputFields), metrics.merge(other.metrics));
  }

  public ModelProfile copy() {
    return merge(null);
  }

  public void track(Map<String, ?> columns) {
    this.metrics.track(columns);
  }
}
