package com.whylogs.core.schemas;

import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.resolvers.Resolver;
import com.whylogs.core.resolvers.StandardResolver;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Data
@EqualsAndHashCode
@Getter
public class DatasetSchema {
  private HashMap<String, Type> types = new HashMap<>();
  private final int LARGE_CACHE_SIZE_LIMIT = 1024 * 100;
  private HashMap<String, ColumnSchema> columns;
  private MetricConfig defaultConfig;
  private Resolver resolver;
  private int cacheSize = 1024;
  private boolean schemaBasedAutomerge = false;

  public DatasetSchema() {
    this(Optional.empty(), Optional.empty());
  }

  public DatasetSchema(Optional<MetricConfig> defaultConfig, Optional<Resolver> resolver) {
    this.columns = new HashMap<>();
    this.defaultConfig = defaultConfig.orElse(new MetricConfig());
    this.resolver = resolver.orElse(new StandardResolver());
  }

  public DatasetSchema(int cacheSize, boolean schemaBasedAutomerge) {
    this.columns = new HashMap<>();
    this.defaultConfig = new MetricConfig();
    this.resolver = new StandardResolver();
    this.cacheSize = cacheSize;
    this.schemaBasedAutomerge = schemaBasedAutomerge;

    if (cacheSize < 0) {
      // TODO: log warning
      this.cacheSize = 0;
    }

    if (cacheSize > LARGE_CACHE_SIZE_LIMIT) {
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

  private DatasetSchema(DatasetSchema other) {
    this.types = new HashMap<>(other.types);
    this.defaultConfig = other.getDefaultConfig().copy();
    this.resolver = other.getResolver().copy();
    this.cacheSize = other.getCacheSize();
    this.schemaBasedAutomerge = other.isSchemaBasedAutomerge();

    this.columns = new HashMap<>();
    for (String colName : other.getColumns().keySet()) {
      this.columns.put(colName, other.getColumns().get(colName).copy());
    }
  }

  public DatasetSchema copy() {
    return new DatasetSchema(this);
  }

  public boolean resolve(HashMap<String, ?> data) {
    boolean dirty = false;
    for (String columnName : data.keySet()) {
      if (this.columns.containsKey(columnName)) {
        continue;
      }

      this.columns.put(
          columnName,
          new ColumnSchema(data.get(columnName).getClass(), this.defaultConfig, this.resolver));

      dirty = true;
    }
    return dirty;
  }

  public Optional<ColumnSchema> get(String name) {
    return Optional.ofNullable(this.columns.get(name));
  }

  public Set<String> getColNames() {
    return this.columns.keySet();
  }
}
