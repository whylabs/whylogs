package com.whylogs.core.schemas;

import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.resolvers.Resolver;
import lombok.Data;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

@Data
public class DatasetSchema {

    private HashMap<String, Type> types = new HashMap<>();
    private final int LARGE_CACHE_SIZE_LIMIT = 1024 * 100;
    public HashMap<String, ColumnSchema> columns;
    public MetricConfig defaultConfig;
    // TODO: typemapper
    public Resolver resolver;
    public int cache_size = 1024;
    public boolean schema_based_automerge = false;

    public DatasetSchema() {
        this.columns = new HashMap<>();
        this.defaultConfig = new MetricConfig();
    }

    public DatasetSchema(int cache_size , boolean schema_based_automerge) {
        this.columns = new HashMap<>();
        this.defaultConfig = new MetricConfig();
        this.cache_size = cache_size;
        this.schema_based_automerge = schema_based_automerge;

        if(cache_size < 0) {
            // TODO: log warning
            this.cache_size = 0;
        }

        if(cache_size > LARGE_CACHE_SIZE_LIMIT) {
            // TODO: log warning
        }

        if(!this.types.isEmpty()){
            for(String key : this.types.keySet()){
                this.columns.put(key, new ColumnSchema(this.types.get(key), this.defaultConfig, this.resolver));
            }
        }
    }

    // TODO: java version of post init

    public DatasetSchema copy() {
        DatasetSchema copy = new DatasetSchema();
        // TODO: copy over

        return copy;
    }

    public boolean resolve(HashMap<String, ?> data) {
        for (String key : data.keySet()) {
            if(this.columns.containsKey(key)) {
              continue;
            }

            this.columns.put(key, new ColumnSchema(
                    data.get(key).getClass(),
                    this.defaultConfig,
                    this.resolver
            ));
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
