package com.whylogs.core.metrics.components;

import com.whylogs.core.message.MetricComponentMessage;
import lombok.*;

/***
A metric component is the smallest unit for a metric.

A metric might consist of multiple components. An example is distribution metric, which consists of kll sketch for
histogram, mean and m2. The calculation of components could be independent or could be coupled with other
components.
***/
@Getter
@EqualsAndHashCode(callSuper=false)
public class MetricComponent<T> {
    private static final ComponentTypeID TYPE_ID = ComponentTypeID.METRIC; // Maybe don't look at this as final if serializer, double check

    @NonNull
    private final T value;
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

    public ComponentTypeID getTypeId() {
        return TYPE_ID;
    }

    public MetricComponent<T> add(MetricComponent<T> other) {
        // TODO this is where we will use the aggregators
        return other;
    }



    // TODO to_protobuf
    // TODO from_protobuf
    // TODO: add a from_protobuf iwht registries passed in
    public static <T extends MetricComponent> T from_protobuf(MetricComponentMessage message) {
        // TODO: check that it's a MetricComponent dataclass
        return null;
    }

}
