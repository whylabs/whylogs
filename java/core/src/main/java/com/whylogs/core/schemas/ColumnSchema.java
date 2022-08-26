package com.whylogs.core.schemas;

import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.resolvers.Resolver;
import java.lang.reflect.Type;
import java.util.HashMap;
import lombok.Data;

@Data
public class ColumnSchema {
  // Thoughts: we could have this ColumnSchema<T> instead of having it as a member
  // bu this might be easier to use? If we did we would need to use the CRTP again
  // like in Metric to be able to see the type but also have them in a collection togehter
  Type type;
  MetricConfig config;
  Resolver resolver;

  public ColumnSchema(Type type, MetricConfig config, Resolver resolver) {
    this.type = type;
    this.config = config;
    this.resolver = resolver;
  }

  public <T extends Metric> HashMap<String, T> getMetrics() {
    return this.resolver.resolve(this);
  }
}
