package com.whylogs.core.metrics;

public enum StandardMetricFactory {
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
}
