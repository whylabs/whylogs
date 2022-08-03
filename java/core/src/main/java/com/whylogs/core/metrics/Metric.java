package com.whylogs.core.metrics;

import com.whylogs.core.PreProcessedColumn;
import com.whylogs.core.SummaryConfig;
import lombok.Data;

import java.util.HashMap;

@Data
public abstract class Metric{

    private String namespace;

    // TODO: set the class to have a registry when initialized

    public static String getNameSpace(Metric metric, MetricConfig config){
        return metric.namespace;
    }

    public abstract <T extends Metric> T zero(MetricConfig config);

    public <T extends Metric> T add(Class<T> otherMetric){
        return this.merge(otherMetric);
    }

    public <T extends Metric> T merge(Class<T> otherMetric){
        // TODO: Metric Components
        return null;
    }

    // TODO: protobuf needs MetricMessage

    // TODO: get_component_paths needs MetricComponents

    public abstract <O> HashMap<String, O> toSummaryDict(SummaryConfig config); // TODO: this doesn't make a good api so ...
    public abstract OperationResult columnarUpdate(PreProcessedColumn data);

    /* TODO: Not ready for this yet
    public static <T extends Metric> T from_protobuf(MetricMessage message){
        // Todo: check that it's a Metric dataclass

        HashMap<String, Class<M extends MetricComponent<?>> M> components = new HashMap<>();
        for k, m in message.components.items():
            components[k] = MetricComponent.from_protobuf(m);

        // TODO: We will have to figure out how to do this to make sure it's not just a Metric
        // but the sublcass
        //MethodHandles.lookup().lookupClass()
        return null;
    }*/
}