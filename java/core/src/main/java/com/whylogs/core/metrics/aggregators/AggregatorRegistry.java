package com.whylogs.core.metrics.aggregators;

import com.whylogs.core.metrics.components.MetricComponent;
import lombok.Data;

import java.util.HashMap;

@Data
public class AggregatorRegistry {
    private HashMap<String, IAggregator> namedAggregators;
    private HashMap<Integer, IAggregator> idAggregators;

    public <T extends MetricComponent> void register(T component, IAggregator aggregator) {
        idAggregators.put(component.getTypeId(), aggregator);
        namedAggregators.put(component.getTypeName(), aggregator);
    }

    public void register(int typeId, IAggregator aggregator) {
        idAggregators.put(typeId, aggregator);
    }

    public IAggregator get(MetricComponent component) {
        return idAggregators.get(component.getTypeId());
    }

    public IAggregator get(int typeId) {
        return idAggregators.get(typeId);
    }

    public IAggregator get(String typeName) {
        return namedAggregators.get(typeName);
    }
}
