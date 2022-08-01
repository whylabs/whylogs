package com.whylogs.core.metics;

import com.whylogs.core.PreProcessedColumn;
import com.whylogs.core.metics.components.MetricComponent;
import org.apache.commons.lang3.NotImplementedException;

import java.util.HashMap;

public abstract class Metric{

    private String namespace;

    // TODO: needs a __post_init__ for the registry

    public static String getNameSpace(Metric metric, MetricConfig config){
        return metric.zero(config).namespace;
    }

    public abstract <T extends Metric> T zero(MetricConfig config);

    public <T extends Metric> T add(Class<T extends Metric> otherMetric){
        return this.merge(otherMetric);
    }

    public <T extends Metric> T merge(Class<T extends Metric> otherMetric){
        // TODO: Metric Components
    }

    // TODO: protobuf needs MetricMessage

    // TODO: get_component_paths needs MetricComponents

    public abstract HashMap<String, ?> toSummaryDict(SummaryConfig config);
    public abstract OperationResult columnarUpdate(PreProcessedColumn data);

    public static <T extends Metric> T from_protobuf(MetricMessage message){
        // Todo: check that it's a Metric dataclass
        /*
        HashMap<String, Class<M extends MetricComponent<?>> M> components = new HashMap<>();
        for k, m in message.components.items():
            components[k] = MetricComponent.from_protobuf(m);
        */
        // TODO: We will have to figure out how to do this to make sure it's not just a Metric
        // but the sublcass
        return null;
    }
}