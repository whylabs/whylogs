package com.whylogs.core.metrics;

import lombok.NonNull;

public abstract class BaseMetric<TSubclass extends BaseMetric> extends Metric {

    public BaseMetric(@NonNull String namespace) {
        super(namespace);
    }

    public abstract TSubclass merge(TSubclass other);
    // public abstract TSubclass fromProtobuf(MetricMessage message); TODO: this will need to be moved to a factory
}
