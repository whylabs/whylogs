package com.whylogs.core.metics.components;

import com.whylogs.core.message.MetricComponentMessage;
import lombok.Data;

/***
A metric component is the smallest unit for a metric.

A metric might consist of multiple components. An example is distribution metric, which consists of kll sketch for
histogram, mean and m2. The calculation of components could be independent or could be coupled with other
components.
***/
@Data
public class MetricComponent<T> {
    private static final int type_id = 0;
    private final T value;
    // TODO: add fields
    // Add registry to this class
    // add serializer
    // add deserialier
    /// add aggregator
    // TODO do we need the optional mtype? (we shouldn't in java?)

    public MetricComponent(T value) {
        this.value = value;

        // init the registries
        // TODO: lots of aggregators, deserializers, serializers, etc.

    }

    public MetricComponent<T> add(MetricComponent<T> other) {
        // TODO this is where we will use the aggregators
        return other;
    }


    // TODO to_protobuf
    // TODO from_protobuf
    public static <T extends MetricComponent> T from_protobuf(MetricComponentMessage message) {
        // TODO: check that it's a MetricComponent dataclass
        return null;
    }

    // TODO: add a from_protobuf iwht registries passed in



}
