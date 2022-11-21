package com.whylogs.core.resolvers;

import com.whylogs.core.DataTypes;
import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.schemas.ColumnSchema;
import java.util.HashMap;
import org.apache.commons.lang3.NotImplementedException;

public class StandardResolver extends Resolver {
  public StandardResolver() {
    super();
  }

  // TODO: the rest of the metrics need implmeented
  @Override
  public HashMap<String, Metric<?>> resolve(ColumnSchema schema) {
    HashMap<String, Metric<?>> resolvedMetrics = new HashMap<>();

    if (DataTypes.Integral.includes(schema.getType())) {
      resolvedMetrics.put(IntegralMetric.NAMESPACE, IntegralMetric.zero(schema.getConfig()));
    } else if (DataTypes.Fractional.includes(schema.getType())) {
      throw new NotImplementedException("Fractional metrics not implemented");
    } else if (DataTypes.String.includes(schema.getType())) {
      if (schema.getConfig().isTrack_unicode_ranges()) {
        throw new NotImplementedException("String & Unicode metrics not implemented");
      }
    }

    if (schema.getConfig().isFi_disabled()) {
      throw new NotImplementedException("Frequent Items metrics not implemented");
    }
    return resolvedMetrics;
  }
}
