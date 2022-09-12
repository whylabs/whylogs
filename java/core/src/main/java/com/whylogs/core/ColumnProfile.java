package com.whylogs.core;

import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.OperationResult;
import com.whylogs.core.schemas.ColumnSchema;
import com.whylogs.core.views.ColumnProfileView;
import java.util.ArrayList;
import java.util.HashMap;

public class ColumnProfile<T> {
  // Required
  private String name;
  private ColumnSchema schema;
  private int cachedSize;

  // Has Defaults
  private HashMap<String, Metric> metrics;
  private SingleFieldProjector projector;
  private int successCount;
  private int failureCount;

  private ArrayList<T> cachedValues;

  public ColumnProfile(String name, ColumnSchema schema, int cachedSize) {
    this.name = name;
    this.schema = schema;
    this.cachedSize = cachedSize; // TODO: add logger for size of cache on column

    // Defaulted
    this.metrics = new HashMap<>();
    this.projector = new SingleFieldProjector(name);
    this.successCount = 0;
    this.failureCount = 0;
    this.cachedValues = new ArrayList<>();
  }

  public void addMetric(Metric metric) {
    if (this.metrics.containsKey(metric.getNamespace())) {
      // TODO: Add logger with warning about replacement
    }

    this.metrics.put(metric.getNamespace(), metric);
  }

  public void track(HashMap<String, T> row) {
    T value = this.projector.apply(row);
    this.cachedValues.add(value);

    if (this.cachedValues.size() >= this.cachedSize) {
      this.flush();
    }
  }

  public void flush() {
    // TODO: Logger was initially here, but only for when it was forced, think it through
    ArrayList<T> oldCache = this.cachedValues;
    this.cachedValues = new ArrayList<>();
    this.trackColumn(oldCache);
  }

  public void trackColumn(ArrayList<?> values) {
    PreprocessedColumn proccessedColumn = PreprocessedColumn.apply(values);

    for (Metric metric : this.metrics.values()) {
      OperationResult result = metric.columnarUpdate(proccessedColumn);
      this.successCount += result.getSuccesses();
      this.failureCount += result.getFailures();
    }
  }

  public ColumnProfileView view() {
    this.flush();
    return new ColumnProfileView(this.metrics, this.successCount, this.failureCount);
  }
}
