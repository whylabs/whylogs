package com.whylogs.core.views;

import java.time.Instant;
import java.util.*;
import lombok.Getter;
import lombok.ToString;

// TODO: extend writable when we do Protobuf
@Getter
@ToString
public class DatasetProfileView {
  private HashMap<String, ColumnProfileView> columns;
  private Instant datasetTimestamp;
  private Instant creationTimestamp;

  public DatasetProfileView(HashMap<String, ColumnProfileView> columns) {
    this(columns, Instant.now(), Instant.now());
  }

  public DatasetProfileView(
      HashMap<String, ColumnProfileView> columns,
      Instant datasetTimestamp,
      Instant creationTimestamp) {
    this.columns = columns;
    this.datasetTimestamp = datasetTimestamp;
    this.creationTimestamp = creationTimestamp;
  }

  public DatasetProfileView merge(DatasetProfileView otherView) {
    if (otherView == null) {
      return this;
    }

    HashMap<String, ColumnProfileView> mergedColumns = new HashMap<>();
    HashSet<String> allNames = new HashSet<>();
    allNames.addAll(this.columns.keySet());
    allNames.addAll(otherView.columns.keySet());

    for (String columnName : allNames) {
      ColumnProfileView thisColumn = this.columns.get(columnName);
      ColumnProfileView otherColumn = otherView.columns.get(columnName);

      ColumnProfileView result = thisColumn;

      if (thisColumn != null && otherColumn != null) {
        result = thisColumn.merge(otherColumn);
      } else if (otherColumn != null) {
        result = otherColumn;
      }
      mergedColumns.put(columnName, result);
    }

    return new DatasetProfileView(mergedColumns, this.datasetTimestamp, this.creationTimestamp);
  }

  public Optional<ColumnProfileView> getColumn(String columnName) {
    return Optional.ofNullable(this.columns.get(columnName));
  }

  public Map<String, ColumnProfileView> getColumns(Optional<ArrayList<String>> colNames) {
    if (colNames.isPresent()) {
      HashMap<String, ColumnProfileView> result = new HashMap<>();
      for (String colName : colNames.get()) {
        result.put(colName, this.columns.get(colName));
      }
      return Collections.unmodifiableMap(result);
    } else {
      return Collections.unmodifiableMap(this.columns);
    }
  }
}
