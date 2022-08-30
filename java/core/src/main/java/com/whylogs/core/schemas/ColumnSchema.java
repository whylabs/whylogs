package com.whylogs.core.schemas;

import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.resolvers.Resolver;
import java.lang.reflect.Type;
import java.util.HashMap;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data @AllArgsConstructor
public class ColumnSchema {
  // Thoughts: we could have this ColumnSchema<T> instead of having it as a member
  // bu this might be easier to use? If we did we would need to use the CRTP again
  // like in Metric to be able to see the type but also have them in a collection togehter
  private Type type;
  private MetricConfig config;
  private Resolver resolver;

  public <T extends Metric> HashMap<String, T> getMetrics() {
    return this.resolver.resolve(this);
  }
}
