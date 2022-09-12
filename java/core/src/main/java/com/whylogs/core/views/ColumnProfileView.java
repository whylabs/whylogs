package com.whylogs.core.views;

import com.whylogs.core.SummaryConfig;
import com.whylogs.core.errors.UnsupportedError;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.components.MetricComponent;
import java.util.*;
import lombok.Getter;

@Getter
public class ColumnProfileView {
  private HashMap<String, Metric> metrics;
  private int successes = 0;
  private int failures = 0;

  public ColumnProfileView(HashMap<String, Metric> metrics) {
    this.metrics = metrics;
  }

  public ColumnProfileView(HashMap<String, Metric> metrics, int successes, int failures) {
    this.metrics = metrics;
    this.successes = successes;
    this.failures = failures;
  }

  public ColumnProfileView merge(ColumnProfileView otherView) {
    HashSet<String> allMetricNames = new HashSet<>();
    allMetricNames.addAll(this.metrics.keySet());
    allMetricNames.addAll(otherView.metrics.keySet());

    HashMap<String, Metric> mergedMetrics = new HashMap<>();
    for (String metricName : allMetricNames) {
      Metric thisMetric = this.metrics.get(metricName);
      Metric otherMetric = otherView.metrics.get(metricName);

      Metric result = thisMetric;

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

  public Optional<Metric> getMetric(String metricName) {
    return Optional.ofNullable(this.metrics.get(metricName));
  }

  public static ColumnProfileView zero() {
    return new ColumnProfileView(new HashMap<>());
  }

  // TODO: metric needs a getComponentPath
  public ArrayList<String> getMetricComponentPaths() {
    ArrayList<String> paths = new ArrayList<>();
    for (String metricName : this.getMetricNames()) {
      Optional<Metric> metric = this.getMetric(metricName);
      if (metric.isPresent()) {
        for (String componentName : metric.get().getComponents().keySet()) {
          paths.add(metricName + "/" + componentName);
        }
      }
    }
    return paths;
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

    // TODO: there was a logger for when a ssummary couldn't be implmented for a metric

    if (columnMetric.isPresent() && columnMetric.get().length() == 0) {
      throw new UnsupportedError(
          "No metric available for requested column metric: " + columnMetric.get());
    }

    return summary;
  }

  private HashMap<String, Object> getMetricSummaryHelper(
      SummaryConfig summaryConfig, Optional<Metric> maybeMetric) {
    HashMap<String, Object> result = new HashMap<>();
    Metric metric;
    if (maybeMetric.isPresent()) {
      metric = maybeMetric.get();
      HashMap<String, Object> metricSummary = metric.toSummaryDict(summaryConfig);
      for (String componentName : metricSummary.keySet()) {
        String fullName = metric.getNamespace() + "/" + componentName;
        result.put(fullName, metricSummary.get(componentName));
      }
    }
    return result;
  }

  public Map<String, MetricComponent> getComponents() {
    HashMap<String, MetricComponent> result = new HashMap<>();
    for (String metricName : this.getMetricNames()) {
      Optional<Metric> metric = this.getMetric(metricName);
      if (metric.isPresent()) {
        result.putAll(metric.get().getComponents());
      }
    }
    return result;
  }
}
