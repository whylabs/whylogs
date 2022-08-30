package com.whylogs.core.resolvers;

import com.whylogs.core.DataTypes;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.StandardMetric;
import com.whylogs.core.schemas.ColumnSchema;
import java.util.ArrayList;
import java.util.HashMap;

public class StandardResolver extends Resolver {
  public StandardResolver() {
    super();
  }

  // TODO: the rest of the metrics need implmeented
  // TODO: does this Metric loose it's typing?
  @Override
  public HashMap<String, Metric> resolve(ColumnSchema schema) {
    ArrayList<StandardMetric> standardMetrics = new ArrayList<>();
    // standardMetrics.add(StandardMetric.counts, StandardMetric.types)

    if (DataTypes.Integral.includes(schema.getType())) {
      standardMetrics.add(StandardMetric.ints);
      // standardMetrics.add(StandardMetric.distribution);
      // standardMetrics.add(StandardMetric.cardinality);
      // standardMetrics.add(StandardMetric.frequent_items);
    } else if (DataTypes.Fractional.includes(schema.getType())) {
      // standardMetrics.add(StandardMetric.distribution);
      // standardMetrics.add(StandardMetric.cardinality);
    } else if (DataTypes.String.includes(schema.getType())) {
      // standardMetrics.add(StandardMetric.cardinality);
      // standardMetrics.add(StandardMetric.distribution);
      // standardMetrics.add(StandardMetric.frequent_items);

      if (schema.getConfig().isTrack_unicode_ranges()) {
        // standardMetrics.add(StandardMetric.unicode_range);
      }
    }

    if (schema.getConfig().isFi_disabled()) {
      // standardMetrics.remove(StandardMetric.frequent_items);
    }

    HashMap<String, Metric> result = new HashMap<>();

    for (StandardMetric metric : standardMetrics) {
      result.put(metric.name(), StandardMetric.getMetric(metric, schema.getConfig()));
    }

    return result;
  }
}
