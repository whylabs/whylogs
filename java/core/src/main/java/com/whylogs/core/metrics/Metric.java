package com.whylogs.core.metrics;

import com.whylogs.core.PreprocessedColumn;
import com.whylogs.core.SummaryConfig;
import com.whylogs.core.metrics.components.MetricComponent;
import java.util.HashMap;
import lombok.*;

@EqualsAndHashCode
@Getter
@Setter
@RequiredArgsConstructor
public abstract class Metric<TSubclass extends Metric> {

  @NonNull private String namespace;

  public abstract HashMap<String, Object> toSummaryDict();

  public abstract HashMap<String, Object> toSummaryDict(SummaryConfig config);

  public abstract OperationResult columnarUpdate(PreprocessedColumn data);

  public abstract HashMap<String, MetricComponent> getComponents();

  public abstract TSubclass merge(Metric<?> other);

  public @NonNull String getNamespace() {
    return namespace;
  }
}
