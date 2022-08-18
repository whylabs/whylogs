package whylogs.core.metrics.components;

import lombok.*;
import org.apache.commons.lang3.NotImplementedException;
import whylogs.core.message.MetricComponentMessage;

/**
 * * A metric component is the smallest unit for a metric.
 *
 * <p>A metric might consist of multiple components. An example is distribution metric, which
 * consists of kll sketch for histogram, mean and m2. The calculation of components could be
 * independent or could be coupled with other components. *
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class MetricComponent<T> {
  private static final int TYPE_ID =
      0; // Maybe don't look at this as final if serializer, double check

  @NonNull private final T value;
  // TODO: add fields
  // Add registry to this class
  // add serializer
  // add deserialier
  /// add aggregator: https://github.com/whylabs/whylogs/pull/719#discussion_r936202557

  public MetricComponent(@NonNull T value) {
    this.value = value;

    // init the registries
    // TODO: lots of aggregators, deserializers, serializers, etc.

  }

  public @NonNull T getValue() {
    return value;
  }

  public int getTypeId() {
    return TYPE_ID;
  }

  public MetricComponent<T> merge(MetricComponent<T> other) {
    // TODO this is where we will use the aggregators
    throw new NotImplementedException();
  }

  // TODO to_protobuf
  // TODO from_protobuf
  // TODO: add a from_protobuf iwht registries passed in
  public static <T extends MetricComponent> T from_protobuf(MetricComponentMessage message) {
    // TODO: check that it's a MetricComponent dataclass
    throw new NotImplementedException();
  }
}
