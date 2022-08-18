package whylogs.core.metrics;

import lombok.NonNull;

// Wrapper allows for the use of the Metric class be in collections without losing
// the type. This allows for the merge and other methods to return the correct type.
public abstract class AbstractMetric<TSubclass extends AbstractMetric> extends Metric {

  public AbstractMetric(@NonNull String namespace) {
    super(namespace);
  }

  public abstract TSubclass merge(TSubclass other);
  // public abstract TSubclass fromProtobuf(MetricMessage message); TODO: this will need to be moved
  // to a factory
}
