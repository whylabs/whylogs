package com.whylogs.core;

import com.google.common.collect.Sets;
import com.whylogs.core.message.ModelProfileMessage;
import com.whylogs.core.message.ModelType;
import com.whylogs.core.metrics.ModelMetrics;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ModelProfile {
  private final Set<String> outputFields;
  @NonNull @Getter private final ModelMetrics metrics;

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
    builder.setMetrics(metrics.toProtobuf());
    return builder;
  }

  @Nullable
  public static ModelProfile fromProtobuf(@Nullable ModelProfileMessage message) {
    if (message == null
        || message.getSerializedSize() == 0
        || message.getMetrics().getModelType().equals(ModelType.UNKNOWN)) {
      return null;
    }

    val metrics = ModelMetrics.fromProtobuf(message.getMetrics());
    if (metrics == null) {
      return null;
    }
    val outputFields = Sets.newHashSet(message.getOutputFieldsList());
    if (outputFields.isEmpty()) {
      log.warn("Empty output fields. This is not supposed to happen");
    }

    return new ModelProfile(outputFields, metrics);
  }

  @NonNull
  public ModelProfile merge(@Nullable ModelProfile other) {
    if (other == null) {
      val metricsCopy = metrics.copy();
      return new ModelProfile(Sets.newHashSet(outputFields), metricsCopy);
    }

    if (!this.outputFields.equals(other.outputFields)) {
      log.warn("Output fields don't match. Using the current output fields");
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
