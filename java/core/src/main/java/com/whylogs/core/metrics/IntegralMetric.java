package com.whylogs.core.metrics;

import com.whylogs.core.PreprocessedColumn;
import com.whylogs.core.SummaryConfig;
import com.whylogs.core.message.MetricComponentMessage;
import com.whylogs.core.message.MetricMessage;
import com.whylogs.core.metrics.components.MaxIntegralComponent;
import com.whylogs.core.metrics.components.MetricComponent;
import com.whylogs.core.metrics.components.MinIntegralComponent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = false)
public class IntegralMetric extends Metric<IntegralMetric> {
  public static final String NAMESPACE = "ints";
  private MaxIntegralComponent maxComponent;
  private MinIntegralComponent minComponent;

  public IntegralMetric() {
    super(IntegralMetric.NAMESPACE);
    this.maxComponent = new MaxIntegralComponent(Integer.MIN_VALUE);
    this.minComponent = new MinIntegralComponent(Integer.MAX_VALUE);
  }

  public IntegralMetric(MaxIntegralComponent maxComponent, MinIntegralComponent minComponent) {
    this();

    this.maxComponent = maxComponent.copy();
    this.minComponent = minComponent.copy();
  }

  private void setMax(int max) {
    this.maxComponent = new MaxIntegralComponent(max);
  }

  private void setMin(int min) {
    this.minComponent = new MinIntegralComponent(min);
  }

  @Override
  public OperationResult columnarUpdate(PreprocessedColumn data) {
    if (data.getLength() == 0) {
      return OperationResult.ok();
    }

    int successes = 0;
    int max_ = this.maxComponent.getValue();
    int min_ = this.minComponent.getValue();

    if (data.hasListInts()) {
      ArrayList<Integer> data_list = data.getLists().getInts();
      int l_max = Collections.max(data_list);
      int l_min = Collections.min(data_list);
      max_ = Integer.max(max_, l_max);
      min_ = Integer.min(min_, l_min);
      successes += data_list.size();
    }

    this.setMax(max_);
    this.setMin(min_);
    return OperationResult.status(successes, 0, data.getNullCount());
  }

  @Override
  public HashMap<String, MetricComponent> getComponents() {
    HashMap<String, MetricComponent> components = new HashMap<>();
    components.put(this.maxComponent.getTypeName(), this.maxComponent);
    components.put(this.minComponent.getTypeName(), this.minComponent);
    return components;
  }

  public static IntegralMetric zero(MetricConfig config) {
    return new IntegralMetric();
  }

  public static IntegralMetric zero() {
    return IntegralMetric.zero(new MetricConfig());
  }

  @Override
  public HashMap<String, Object> toSummaryDict() {
    SummaryConfig defaultConfig = new SummaryConfig();
    return this.toSummaryDict(defaultConfig);
  }

  @Override
  public HashMap<String, Object> toSummaryDict(SummaryConfig config) {
    // This metric does not need the config, but others do
    HashMap<String, Object> summary = new HashMap<>();
    summary.put("max", this.maxComponent.getValue());
    summary.put("min", this.minComponent.getValue());
    return summary;
  }

  @Override
  public IntegralMetric merge(Metric<?> other) {
    if (!this.getNamespace().equals(other.getNamespace())) {
      throw new IllegalArgumentException(
          "Cannot merge IntegralMetrics with different namespaces:"
              + this.getNamespace()
              + " and "
              + other.getNamespace());
    }

    IntegralMetric other_ = (IntegralMetric) other;
    int max = Integer.max(this.maxComponent.getValue(), other_.maxComponent.getValue());
    int min = Integer.min(this.minComponent.getValue(), other_.minComponent.getValue());

    return new IntegralMetric(new MaxIntegralComponent(max), new MinIntegralComponent(min));
  }

  public static IntegralMetric fromProtobuf(MetricMessage message) {
    HashMap<String, MetricComponent<?>> components = new HashMap<>();
    for (MetricComponentMessage componentMessage : message.getMetricComponentsMap().values()) {
      MetricComponent<?> component = MetricComponent.fromProtobuf(componentMessage);
      components.put(component.getTypeName(), component);
    }

    MaxIntegralComponent max = (MaxIntegralComponent) components.get("MaxIntegralComponent");
    MinIntegralComponent min = (MinIntegralComponent) components.get("MinIntegralComponent");

    if (max == null || min == null) {
      throw new IllegalArgumentException("IntegralMetric must have max and min components");
    }

    return new IntegralMetric(max, min);
  }
}
