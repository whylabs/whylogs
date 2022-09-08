package com.whylogs.core.metrics;

import com.whylogs.core.PreprocessedColumn;
import com.whylogs.core.SummaryConfig;
import java.util.HashMap;

import com.whylogs.core.metrics.components.MetricComponent;
import lombok.*;

@EqualsAndHashCode
@Getter
@Setter
@RequiredArgsConstructor
public abstract class Metric {

  @NonNull private String namespace;

  public abstract HashMap<String, Object> toSummaryDict();

  public abstract HashMap<String, Object> toSummaryDict(SummaryConfig config);

  public abstract OperationResult columnarUpdate(PreprocessedColumn data);

  public abstract HashMap<String, MetricComponent> getComponents();

  public Metric merge(Metric other){
    Metric merged = this;
    if(!this.namespace.equals(other.namespace)){
      throw new IllegalArgumentException("Cannot merge metrics with different namespaces");
    }

    if(this instanceof IntegralMetric){
      ((IntegralMetric) merged).merge((IntegralMetric) other);
    }
    return merged;
  }

  public @NonNull String getNamespace() {
    return namespace;
  }
}
