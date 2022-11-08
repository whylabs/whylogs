package com.whylogs.core.metrics.aggregators;

import com.whylogs.core.metrics.components.IntegralComponent;
import com.whylogs.core.metrics.components.MetricComponent;
import java.util.HashMap;
import java.util.function.BiFunction;

// Registry for aggregators. You can register a component or a type-id to an aggregator.
// The aggregators are of IAggregator which is just a wrapper on BiFunction.
// This allows the use of lambdas to create aggregators.
public class AggregatorRegistry {
  private final HashMap<String, BiFunction> namedAggregators;
  private final HashMap<Integer, BiFunction> idAggregators;

  private AggregatorRegistry() {
    this.namedAggregators = new HashMap<>();
    this.idAggregators = new HashMap<>();

    // Numbers
    // TODO: use the typing for this instead of name
    this.namedAggregators.put("number", new NumberSumAggregator<Number>());
    this.idAggregators.put(0, new NumberSumAggregator<Number>());
    this.idAggregators.put(1, new NumberSumAggregator<Integer>());
    this.idAggregators.put(2, new NumberSumAggregator<Integer>());


  }

  // Instance holder to avoid double checking anti-pattern for singlton
  private static final class InstanceHolder {
    static final AggregatorRegistry instance = new AggregatorRegistry();
  }

  public static AggregatorRegistry getInstance() {
    return InstanceHolder.instance;
  }

  public <T extends MetricComponent, A extends BiFunction> void register(
      T component, A aggregator) {
    idAggregators.put(component.getTypeId(), aggregator);
    namedAggregators.put(component.getTypeName(), aggregator);
  }

  public <A extends BiFunction> void register(int typeId, A aggregator) {
    idAggregators.put(typeId, aggregator);
  }

  public BiFunction get(MetricComponent component) {
    return idAggregators.get(component.getTypeId());
  }

  public BiFunction get(int typeId) {
    return idAggregators.get(typeId);
  }

  public BiFunction get(String typeName) {
    return namedAggregators.get(typeName);
  }
}
