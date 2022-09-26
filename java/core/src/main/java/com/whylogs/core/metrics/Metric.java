package com.whylogs.core.metrics;

import com.whylogs.core.PreprocessedColumn;
import com.whylogs.core.SummaryConfig;
import com.whylogs.core.metrics.components.MetricComponent;
import java.util.HashMap;
import lombok.*;
import org.apache.commons.lang3.NotImplementedException;

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

  public Metric merge(Metric other) {
    if (!this.namespace.equals(other.namespace)) {
      throw new IllegalArgumentException("Cannot merge metrics with different namespaces");
    }

    if (this instanceof IntegralMetric) {
      return ((IntegralMetric) this).merge((IntegralMetric) other);
    }

    throw new NotImplementedException("Cannot merge metrics of type " + this.getClass().getName());
  }

  public @NonNull String getNamespace() {
    return namespace;
  }
}
