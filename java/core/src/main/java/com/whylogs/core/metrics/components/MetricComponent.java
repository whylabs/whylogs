package com.whylogs.core.metrics.components;

import com.whylogs.core.message.MetricComponentMessage;
import com.whylogs.core.metrics.Registries;
import com.whylogs.core.metrics.deserializers.Deserializable;
import com.whylogs.core.metrics.serializers.Serializable;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import lombok.*;
import org.apache.commons.lang3.NotImplementedException;

import java.util.function.BiFunction;

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
  // Maybe don't look at this as final if serializer, double check
  private static int TYPE_ID = 0;

  @NonNull private final T value;

  private Registries registries;
  private Serializable<T> serializer;
  private Deserializable<?> deserializer;
  private BiFunction aggregator;


  public MetricComponent(@NonNull T value) {
    this.value = value;

    this.registries = Registries.getInstance();
    this.serializer = registries.getSerializerRegistry().get(this.getTypeId());
    this.deserializer = registries.getDeserializerRegistry().get(this.getTypeId());
    this.aggregator = registries.getAggregatorRegistry().get(this.getTypeId());

    if(this.serializer == null || this.deserializer == null) {
      throw new ValueException("Serializer and deserializer must be defined in pairs, but serializer is None");
    }
  }

  public @NonNull T getValue() {
    return value;
  }

  public int getTypeId() {
    return TYPE_ID;
  }

  public String getTypeName() {
    return this.getClass().getSimpleName();
  }

  public MetricComponent<T> copy() {
    return new MetricComponent<T>(value);
  }

  public MetricComponent<T> merge(MetricComponent<T> other) {
    // TODO this is where we will use the aggregators
    throw new NotImplementedException();
  }


  public MetricComponentMessage toProtobuf(){
    if(this.serializer == null) {
      throw new ValueException("Serializer must be defined");
    }

    MetricComponentMessage.Builder builder=  this.serializer.serialize(this.value);
    builder.setTypeId(this.getTypeId());
    return builder.build();
  };

  // TODO from_protobuf
  // TODO: add a from_protobuf iwht registries passed in
  public static <M extends MetricComponent> M from_protobuf(MetricComponentMessage message) {
    // TODO: check that it's a MetricComponent dataclass
    throw new NotImplementedException();
  }
}
