package com.whylogs.core;

import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.OperationResult;
import com.whylogs.core.schemas.ColumnSchema;
import com.whylogs.core.views.ColumnProfileView;
import java.util.ArrayList;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
public class ColumnProfile<T> implements AutoCloseable {
  // Required
  private final String name;
  private final ColumnSchema schema;
  private final int cacheSize;

  // Has Defaults
  private HashMap<String, Metric<?>> metrics;
  private final SingleFieldProjector<T> projector;
  private int successCount;
  private int failureCount;
  private int nullCount;
  private ArrayList<T> cachedValues;

  private ColumnProfile(
      String name,
      ColumnSchema schema,
      SingleFieldProjector<T> projector,
      int cacheSize,
      @NonNull HashMap<String, Metric<?>> metrics,
      int successCount,
      int failureCount,
      int nullCount,
      @NonNull ArrayList<T> cachedValues) {
    this.name = name;
    this.schema = schema; // todo: do a copy here
    this.projector = projector;
    this.cacheSize = cacheSize;

    this.metrics = this.schema.getMetrics();
    for (Metric<?> metric : metrics.values()) {
      this.metrics.put(metric.getNamespace(), metric.copy());
    }

    this.successCount = successCount;
    this.failureCount = failureCount;
    this.nullCount = nullCount;
    this.cachedValues = new ArrayList<>(cachedValues);
  }

  public ColumnProfile(String name, ColumnSchema schema, int cacheSize) {
    this.name = name;
    this.schema = schema;
    this.cacheSize = cacheSize; // TODO: add logger for size of cache on column

    // Defaulted
    this.metrics = this.schema.getMetrics();
    this.projector = new SingleFieldProjector<>(name);
    this.successCount = 0;
    this.failureCount = 0;
    this.nullCount = 0;
    this.cachedValues = new ArrayList<>();
  }

  public void addMetric(Metric<?> metric) {
    if (this.metrics.containsKey(metric.getNamespace())) {
      // TODO: Add logger with warning about replacement
    }

    this.metrics.put(metric.getNamespace(), metric);
    // TODO: Wouldn't this implement a success count here?
  }

  // TODO: this only gets one not every part of the row. Should projector actually do it multiple?
  public void track(HashMap<String, Object> row) {
    T value = this.projector.apply(row);
    this.cachedValues.add(value);

    if (this.cachedValues.size() >= this.cacheSize) {
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

    for (Metric<?> metric : this.metrics.values()) {
      OperationResult result = metric.columnarUpdate(proccessedColumn);
      this.successCount += result.getSuccesses();
      this.failureCount += result.getFailures();
      this.nullCount += result.getNulls();
    }
  }

  public ColumnProfileView view() {
    this.flush();
    return new ColumnProfileView(this.metrics, this.successCount, this.failureCount);
  }

  public ColumnProfile<T> copy() {
    return new ColumnProfile<>(
        this.name,
        this.schema,
        this.projector,
        this.cacheSize,
        this.metrics,
        this.successCount,
        this.failureCount,
        this.nullCount,
        this.cachedValues);
  }

  @Override
  public void close() throws Exception {
    this.flush();
  }
}
