from logging import getLogger
from typing import List

import pandas as pd
from python.whylogs.core.constraints.metric_constraint import ConstraintsBuilder, MetricConstraint, MetricsSelector

from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.metrics import DistributionMetric
from whylogs.core.metrics.metrics import Metric, MetricConfig
from whylogs.core.preprocessing import PreprocessedColumn

TEST_LOGGER = getLogger(__name__)

def test_metric_constraint_lambdas() -> None:
    test_integer_column_values = [0,1,2,3,4]
    distribution_metric = DistributionMetric.zero(MetricConfig())
    column_data = PreprocessedColumn.apply(test_integer_column_values)
    distribution_metric.columnar_update(column_data)
    distribution_stddev_between_constraint = MetricConstraint(name="stddev_between_constraint", condition=lambda dist: 1.1 < dist.stddev < 2.3)
    avg_greater_than_two_constraint = MetricConstraint(name="avg_greater_than_two", condition=lambda dist: dist.avg > 2.0)
    TEST_LOGGER.info(f"stddev is {distribution_metric.stddev}")
    assert distribution_stddev_between_constraint.condition(distribution_metric)
    assert not avg_greater_than_two_constraint.condition(distribution_metric)

def test_metric_constraint_callable() -> None:
    test_integer_column_values = [0,1,2,3,4]
    distribution_metric = DistributionMetric.zero(MetricConfig())
    empty_distribution = DistributionMetric.zero(MetricConfig())
    column_data = PreprocessedColumn.apply(test_integer_column_values)
    distribution_metric.columnar_update(column_data)

    def custom_function(metric: DistributionMetric) -> bool:
        c1 = metric.stddev > metric.avg
        c2 = metric.stddev == 0.0
        TEST_LOGGER.info(f"{metric.stddev} > {metric.avg} -> c1:{c1}, c2:{c2}")
        return c1 or c2

    distribution_stddev_gt_avg = MetricConstraint(name="stddev_gt_avg", condition=custom_function)
    TEST_LOGGER.info(f"distribution is {distribution_metric.to_summary_dict()}")
    TEST_LOGGER.info(f"empy distribution is {empty_distribution.to_summary_dict()}")
    assert distribution_stddev_gt_avg.condition(distribution_metric)
    assert distribution_stddev_gt_avg.condition(empty_distribution)

def test_constraints_builder() -> None:
    data = {
        "animal": ["cat", "hawk", "snake", "cat", "mosquito"],
        "legs": [4, 2, 0, 4, 6],
        "weight": [4.3, 1.8, 1.3, 4.1, 5.5e-6],
    }
    df = pd.DataFrame(data)
    profile = DatasetProfile()
    profile.track(pandas=df)
    view = profile.view()
    constraints_builder = ConstraintsBuilder(dataset_profile_view=view)
    selectors = constraints_builder.get_metric_selectors()
    TEST_LOGGER.info(f"selectors are: {selectors}")

    # pick a metric name from available selectors
    metric_selector = MetricsSelector(metric_name="distribution", column_name="legs")
    def metric_resolver(profile_view) -> List[Metric]:
        column_profiles = profile_view.get_columns()
        distribution_metrics = []
        for column_name in column_profiles:
            metric = column_profiles[column_name].get_metric("distribution")
            if metric is not None:
                distribution_metrics.append(metric)
        return distribution_metrics

    constraint_condition = lambda distribution: distribution.max < 12
    legs_less_than_12_constraint = MetricConstraint(
        name="legs less than 12",
        condition=constraint_condition,
        metric_selector=metric_selector)

    distribution_selector = MetricsSelector(metric_name="distribution", metrics_resolver=metric_resolver, )
    no_negative_numbers = MetricConstraint(
        name="no negative numbers",
        condition=lambda numbers: numbers.min >= 0,
        metric_selector=distribution_selector,
        require_column_existence = False) 

    constraints_builder.add_constraint(constraint=legs_less_than_12_constraint)
    constraints_builder.add_constraint(constraint=no_negative_numbers, ignore_missing=True)
    contraints = constraints_builder.execute()
    TEST_LOGGER.info(f"constraints are: {contraints.column_constraints}")
    constraints_valid = contraints.validate()
    report_results = contraints.report()
    TEST_LOGGER.info(f"constraints report is: {report_results}")
    assert constraints_valid

