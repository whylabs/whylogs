package com.whylogs.core;

import com.whylogs.core.metrics.Metric;
import com.whylogs.core.schemas.DatasetSchema;
import com.whylogs.core.views.ColumnProfileView;
import com.whylogs.core.views.DatasetProfileView;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import lombok.Getter;
import lombok.ToString;

// TODO: extend WRITABLE interface
@Getter
@ToString
public class DatasetProfile {
  public static int _LARGE_CACHE_SIZE_LIMIT = 1024 * 100;

  private DatasetSchema schema;
  private Instant datasetTimestamp;
  private Instant creationTimestamp;
  private HashMap<String, ColumnProfile<?>> columns;
  private boolean isActive = false;
  private int trackCount = 0;
  private HashMap<String, Metric<?>> metrics = new HashMap<>();

  // TODO: I don't like how this works for customers. I wouldn't want
  // TODO: to have to pass the optionals around. We should just use overloading instead
  public DatasetProfile(
      DatasetSchema datasetSchema, Instant datasetTimestamp, Instant creationTimestamp) {
    this.schema = datasetSchema;
    this.datasetTimestamp = datasetTimestamp;
    this.creationTimestamp = creationTimestamp;

    this.columns = new HashMap<>();
    this.initializeNewColumns(schema.getColNames());
  }

  public DatasetProfile(DatasetSchema datasetSchema, Instant datasetTimestamp) {
    this(datasetSchema, datasetTimestamp, Instant.now());
  }

  public DatasetProfile(DatasetSchema datasetSchema) {
    this(datasetSchema, Instant.now(), Instant.now());
  }

  public DatasetProfile() {
    this(new DatasetSchema());
  }

  public void addMetric(String colName, Metric<?> metric) {
    if (!this.columns.containsKey(colName)) {
      throw new InputMismatchException("Column name not found in schema");
    }
    this.columns.get(colName).addMetric(metric);
  }

  public void addDatasetMetric(String name, Metric<?> metric) {
    this.metrics.put(name, metric);
  }

  public void track(HashMap<String, Object> row) {
    try {
      this.isActive = true;
      this.trackCount += 1;
      this.doTrack(row);
    } finally {
      this.isActive = false;
    }
  }

  private void doTrack(HashMap<String, ?> row) {
    boolean dirty = this.schema.resolve(row);
    if (dirty) {
      Set<String> schemaColumnNames = this.schema.getColNames();
      Set<String> newColumnNames = new HashSet<>();
      for (String colName : schemaColumnNames) {
        if (!this.columns.containsKey(colName)) {
          newColumnNames.add(colName);
        }
      }
      this.initializeNewColumns(newColumnNames);
    }

    ArrayList<Object> values;
    for (String col : row.keySet()) {
      values = new ArrayList<>();
      values.add(row.get(col));
      this.columns.get(col).trackColumn(values);
    }
  }

  /** @return True if the profile tracking code is currently running. */
  public boolean isEmpty() {
    return this.trackCount == 0;
  }

  public void setDatasetTimestamp(ZonedDateTime datasetTimestamp) {
    if (datasetTimestamp.getZone() == null) {
      // TODO: log warning if it's not there
    }
    this.datasetTimestamp = datasetTimestamp.toInstant();
  }

  public void setDatasetTimestamp(Instant datasetTimestamp) {
    this.datasetTimestamp = datasetTimestamp;
  }

  private void initializeNewColumns(Set<String> colNames) {
    for (String column : colNames) {
      this.schema
          .get(column)
          .ifPresent(
              columnSchema ->
                  this.columns.put(
                      column,
                      new ColumnProfile<>(column, columnSchema, this.schema.getCacheSize())));
    }
  }

  public DatasetProfileView view() {
    HashMap<String, ColumnProfileView> columns = new HashMap<>();

    for (String colName : this.columns.keySet()) {
      columns.put(colName, this.columns.get(colName).view());
    }

    return new DatasetProfileView(columns, this.datasetTimestamp, this.creationTimestamp);
  }

  public void flush() {
    for (String colName : this.columns.keySet()) {
      this.columns.get(colName).flush();
    }
  }

  public static String getDefaultPath(String path) {
    if (!path.endsWith("bin")) {
      return path + "_" + Instant.now().toEpochMilli() + ".bin";
    }

    return path;
  }

  public static String getDefaultPath() {
    return "profile." + Instant.now().toEpochMilli() + ".bin";
  }
}
