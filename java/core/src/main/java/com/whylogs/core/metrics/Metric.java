package com.whylogs.core.metrics;

import com.whylogs.core.PreprocessedColumn;
import com.whylogs.core.SummaryConfig;
import com.whylogs.core.message.MetricComponentMessage;
import com.whylogs.core.message.MetricMessage;
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

  // TODO: This needs to be moved to a factory
  public static Metric<?> fromProtobuf(MetricMessage message, String namespace) {
    switch(namespace) {
      case "ints":
        return IntegralMetric.fromProtobuf(message);

      default:
        throw new IllegalArgumentException("Unknown metric namespace: " + namespace);
    }
  }

  public MetricMessage toProtobuf(){
    MetricMessage.Builder builder = MetricMessage.newBuilder();

    for (String key : getComponents().keySet()) {
      builder.putMetricComponents(key, getComponents().get(key).toProtobuf());
    }

    return builder.build();
  }

}
