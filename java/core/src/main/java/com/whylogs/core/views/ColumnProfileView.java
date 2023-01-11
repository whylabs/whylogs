package com.whylogs.core.views;

import com.whylogs.core.ColumnProfile;
import com.whylogs.core.SummaryConfig;
import com.whylogs.core.errors.UnsupportedError;
import com.whylogs.core.message.ColumnMessage;
import com.whylogs.core.message.MetricComponentMessage;
import com.whylogs.core.message.MetricMessage;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.components.MetricComponent;
import java.util.*;
import lombok.Getter;

@Getter
public class ColumnProfileView {
  private HashMap<String, Metric<?>> metrics;
  private int successes = 0;
  private int failures = 0;

  public ColumnProfileView(HashMap<String, Metric<?>> metrics) {
    this.metrics = metrics;
  }

  public ColumnProfileView(HashMap<String, Metric<?>> metrics, int successes, int failures) {
    this.metrics = new HashMap<>();
    if (metrics != null) {
      this.metrics.putAll(metrics);
    }
    this.successes = successes;
    this.failures = failures;
  }

  public ColumnProfileView merge(ColumnProfileView otherView) {
    if (otherView == null) {
      // TODO: log warning that otehrwas null and this returns original
      return this;
    }

    HashSet<String> allMetricNames = new HashSet<>();
    allMetricNames.addAll(this.metrics.keySet());
    allMetricNames.addAll(otherView.metrics.keySet());

    HashMap<String, Metric<?>> mergedMetrics = new HashMap<>();
    for (String metricName : allMetricNames) {
      Metric<?> thisMetric = this.metrics.get(metricName);
      Metric<?> otherMetric = otherView.metrics.get(metricName);

      Metric<?> result = thisMetric;

      if (thisMetric != null && otherMetric != null) {
        result = thisMetric.merge(otherMetric);
      } else if (otherMetric != null) {
        result = otherMetric;
      }

      mergedMetrics.put(metricName, result);
    }

    return new ColumnProfileView(
        mergedMetrics, this.successes + otherView.successes, this.failures + otherView.failures);
  }

  public Optional<Metric<?>> getMetric(String metricName) {
    return Optional.ofNullable(this.metrics.get(metricName));
  }

  public static ColumnProfileView zero() {
    return new ColumnProfileView(new HashMap<>());
  }

  // TODO: metric needs a getComponentPath
  public List<String> getMetricComponentPaths() {
    ArrayList<String> paths = new ArrayList<>();
    for (String metricName : this.getMetricNames()) {
      Optional<Metric<?>> metric = this.getMetric(metricName);
      if (metric.isPresent()) {
        for (String componentName : metric.get().getComponents().keySet()) {
          paths.add(metricName + "/" + componentName);
        }
      }
    }
    return Collections.unmodifiableList(paths);
  }

  public ArrayList<String> getMetricNames() {
    return new ArrayList<>(this.getMetrics().keySet());
  }

  public HashMap<String, Object> toSummaryDict(
      Optional<String> columnMetric, Optional<SummaryConfig> config) throws UnsupportedError {
    SummaryConfig summaryConfig = config.orElse(new SummaryConfig());
    HashMap<String, Object> summary = new HashMap<>();

    if (columnMetric.isPresent()) {
      summary.putAll(getMetricSummaryHelper(summaryConfig, this.getMetric(columnMetric.get())));
    } else {
      for (String metricName : this.getMetricNames()) {
        summary.putAll(getMetricSummaryHelper(summaryConfig, this.getMetric(metricName)));
      }
    }

    // TODO: there was a logger for when a summary couldn't be implmented for a metric

    if (columnMetric.isPresent() && columnMetric.get().length() == 0) {
      throw new UnsupportedError(
          "No metric available for requested column metric: " + columnMetric.get());
    }
    return summary;
  }

  private Map<String, Object> getMetricSummaryHelper(
      SummaryConfig summaryConfig, Optional<Metric<?>> maybeMetric) {
    HashMap<String, Object> result = new HashMap<>();
    Metric<?> metric;
    if (maybeMetric.isPresent()) {
      metric = maybeMetric.get();
      HashMap<String, Object> metricSummary = metric.toSummaryDict(summaryConfig);
      for (String componentName : metricSummary.keySet()) {
        String fullName = metric.getNamespace() + "/" + componentName;
        result.put(fullName, metricSummary.get(componentName));
      }
    }
    return Collections.unmodifiableMap(result);
  }

  public Map<String, MetricComponent> getComponents() {
    HashMap<String, MetricComponent> result = new HashMap<>();
    for (String metricName : this.getMetricNames()) {
      Optional<Metric<?>> metric = this.getMetric(metricName);
      metric.ifPresent(value -> result.putAll(value.getComponents()));
    }
    return Collections.unmodifiableMap(result);
  }

  public ColumnMessage toProtobuf() {
    ColumnMessage.Builder builder = ColumnMessage.newBuilder();

    HashMap<String, MetricComponentMessage> componentsWithNamespace = new HashMap<>();
    for (String namespace : this.metrics.keySet()) {
      Metric<?> metric = this.metrics.get(namespace);
      MetricMessage metricMessage = metric.toProtobuf();
      for (String componentKey : metricMessage.getMetricComponentsMap().keySet()) {
        componentsWithNamespace.put(
            namespace + "/" + componentKey,
            metricMessage.getMetricComponentsMap().get(componentKey));
      }
    }

    builder.putAllMetricComponents(componentsWithNamespace);
    return builder.build();
  }

  public static ColumnProfileView fromProtobuf(ColumnMessage message) {
    HashMap<String, HashMap<String, MetricComponentMessage>> componentMessages = new HashMap<>();
    HashMap<String, Metric<?>> metrics = new HashMap<>();
    ColumnProfile<?> profile;

    for (String compoundName : message.getMetricComponentsMap().keySet()) {
      String[] parts = compoundName.split("/");
      String namespace = parts[0];
      String componentName = parts[1];
      MetricComponentMessage componentMessage = message.getMetricComponentsMap().get(compoundName);

      componentMessages
          .computeIfAbsent(namespace, k -> new HashMap<>())
          .put(componentName, componentMessage);
    }

    for (String namespace : componentMessages.keySet()) {
      MetricMessage.Builder builder = MetricMessage.newBuilder();
      builder.putAllMetricComponents(componentMessages.get(namespace));
      MetricMessage metricMessage = builder.build();
      metrics.put(namespace, Metric.fromProtobuf(metricMessage, namespace));
    }

    return new ColumnProfileView(metrics);
  }
}
