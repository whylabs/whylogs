package com.whylogs.core.schemas;

import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.resolvers.Resolver;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import lombok.Data;

@Data
public class DatasetSchema {
  private HashMap<String, Type> types = new HashMap<>();
  private final int LARGE_CACHE_SIZE_LIMIT = 1024 * 100;
  public HashMap<String, ColumnSchema> columns;
  public MetricConfig defaultConfig;
  public Resolver resolver;
  public int cache_size = 1024;
  public boolean schema_based_automerge = false;

  public DatasetSchema() {
    this.columns = new HashMap<>();
    this.defaultConfig = new MetricConfig();
  }

  public DatasetSchema(int cache_size, boolean schema_based_automerge) {
    this.columns = new HashMap<>();
    this.defaultConfig = new MetricConfig();
    this.cache_size = cache_size;
    this.schema_based_automerge = schema_based_automerge;

    if (cache_size < 0) {
      // TODO: log warning
      this.cache_size = 0;
    }

    if (cache_size > LARGE_CACHE_SIZE_LIMIT) {
      // TODO: log warning
    }

    // Type name will be used as the column name
    if (!this.types.isEmpty()) {
      for (String typeName : this.types.keySet()) {
        this.columns.put(
            typeName,
            new ColumnSchema(this.types.get(typeName), this.defaultConfig, this.resolver));
      }
    }
  }

  public DatasetSchema copy() {
    DatasetSchema copy = new DatasetSchema();
    // TODO: copy over
    return copy;
  }

  public boolean resolve(HashMap<String, ?> data) {
    for (String columnName : data.keySet()) {
      if (this.columns.containsKey(columnName)) {
        continue;
      }

      this.columns.put(
          columnName,
          new ColumnSchema(data.get(columnName).getClass(), this.defaultConfig, this.resolver));
    }
    return true;
  }

  public Optional<ColumnSchema> get(String name) {
    return Optional.ofNullable(this.columns.get(name));
  }

  public Set<String> getColNames() {
    return this.columns.keySet();
  }
}
