package com.whylogs.core;

import com.whylogs.core.metrics.Metric;
import com.whylogs.core.schemas.ColumnSchema;
import com.whylogs.core.schemas.DatasetSchema;
import com.whylogs.core.views.ColumnProfileView;
import com.whylogs.core.views.DatasetProfileView;
import java.time.ZonedDateTime;
import java.util.*;
import lombok.Getter;
import lombok.ToString;

// TODO: extend WRITABLE interface
@Getter
@ToString
public class DatasetProfile {
  // TODO: Time zone is all mixed up. Fix
  public static int _LARGE_CACHE_SIZE_LIMIT = 1024 * 100;

  private DatasetSchema schema;
  // QUESTION: Do we need zones here? Do we just use UTC?
  private Date datasetTimestamp;
  private Date creationTimestamp;
  private HashMap<String, ColumnProfile<?>> columns;
  private boolean isActive = false;
  private int trackCount = 0;

  public DatasetProfile(
      Optional<DatasetSchema> datasetSchema,
      Optional<Date> datasetaTimestampe,
      Optional<Date> creationTimestampe) {
    this.schema = datasetSchema.orElse(new DatasetSchema());
    this.datasetTimestamp = datasetaTimestampe.orElse(new Date());
    this.creationTimestamp = creationTimestampe.orElse(new Date());

    this.columns = new HashMap<>();
    this.initializeNewColumns(schema.getColNames());
  }

  public void addMetric(String colName, Metric metric) {
    if (!this.columns.containsKey(colName)) {
      throw new InputMismatchException("Column name not found in schema");
    }
    this.columns.get(colName).addMetric(metric);
  }

  public <T> void track(HashMap<String, T> row) {
    try {
      this.isActive = true;
      this.trackCount += 1;
      this.doTrack(row);
    } finally {
      this.isActive = false;
    }
  }

  private <T> void doTrack(HashMap<String, T> row) {
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

    for (String col : row.keySet()) {
      ArrayList<T> values = new ArrayList<>();
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
    this.datasetTimestamp = Date.from(datasetTimestamp.toInstant());
  }

  private void initializeNewColumns(Set<String> colNames) {
    for (String column : colNames) {
      ColumnSchema columnSchema = this.schema.columns.get(column);
      if (columnSchema != null) {
        this.columns.put(column, new ColumnProfile(column, columnSchema, this.schema.cache_size));
      }
      // TODO: log warning 'Encountered a column without schema: %s", col' in an else
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

  public static String getDefaultPath(Optional<String> path) {
    String defaultPath = "profile." + (int) System.currentTimeMillis() + ".bin";

    if (!path.isPresent()) {
      return defaultPath;
    }

    if (!path.get().endsWith("bin")) {
      String finalPath = path.get() + defaultPath;
      return finalPath;
    }

    return path.get();
  }
}
