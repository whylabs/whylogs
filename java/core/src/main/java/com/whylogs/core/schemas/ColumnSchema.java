package com.whylogs.core.schemas;

import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.resolvers.Resolver;
import lombok.Data;

import java.lang.reflect.Type;
import java.util.HashMap;

@Data
public class ColumnSchema {
    // do I need dtype and mapper?
    Type type;
    MetricConfig config;
    Resolver resolver;

    public ColumnSchema(Type type, MetricConfig config, Resolver resolver) {
        this.config = config;
        this.resolver = resolver;
    }

    public HashMap<String, Metric> getMetrics(){
        return this.resolver.resolve(this);
    }
}
