package com.whylogs.core.metrics;

public enum StandardMetric {
  /*
  types {

  },
  distribution{

  },
  counts{

  },*/
  ints {
    @Override
    public IntegralMetric zero(MetricConfig config) {
      return IntegralMetric.zero(config);
    }
  },
/*
cardinality {

},
frequent_items {

},
unicode_range {

},
condition_count{

}*/
;

  abstract <T extends Metric> T zero(MetricConfig config);

  public static <T extends Metric> T getMetric(String name) {
    return StandardMetric.valueOf(name).zero(new MetricConfig());
  }

  public static <T extends Metric> T getMetric(String name, MetricConfig config) {
    return StandardMetric.valueOf(name).zero(config);
  }

  public static <T extends Metric> T getMetric(StandardMetric metric) {
    return metric.zero(new MetricConfig());
  }

  public static <T extends Metric> T getMetric(StandardMetric metric, MetricConfig config) {
    return metric.zero(config);
  }
}
