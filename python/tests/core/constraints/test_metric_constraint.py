from logging import getLogger
import pandas as pd
from python.whylogs.core.constraints.metric_constraint import ConstraintsBuilder, MetricConstraint, NumericConstraint

from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.metrics import DistributionMetric
from whylogs.core.metrics.metrics import MetricConfig
from whylogs.core.preprocessing import PreprocessedColumn

TEST_LOGGER = getLogger(__name__)

def test_numneric_constraint() -> None:
    test_integer_column_values = [0,1,2,3,4]
    integer_metric = DistributionMetric.zero(MetricConfig())
    column_data = PreprocessedColumn.apply(test_integer_column_values)
    integer_metric.columnar_update(column_data)
    numeric_constraint = NumericConstraint()
    numeric_constraint.max.lower_bound = 3
    numeric_constraint.max.upper_bound = 10
    assert numeric_constraint.validate(integer_metric)

    numeric_constraint.max.lower_bound = 5
    assert not numeric_constraint.validate(integer_metric)

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
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    profile = DatasetProfile()
    profile.track(pandas=df)
    view = profile.view()
    constraints_builder = ConstraintsBuilder(dataset_profile_view=view)

    constraints_builder.add_constraint(column_name="col1", max_less_than=3) # <- signature hard to infer what is valid
    #constraints_builder.add_constraint(column_name="col1", ctype="numeric", max_less_than=3)
    # constraints_builder.add_constraint(column_name="col10", constraint=MetricConstraint(condition=lambda x: x % 2 == 0)) # Andy votes to include
    # constraints_builder.add_numeric_constraint() # Some kind of namespacing
    # constraints_builder.add_text_constraint()

    # multi column, apply to multiple columns

    # recipes for simple constraints.
    # could create a MetricConstraint <- Andy and Felipe

    contraints = constraints_builder.execute()
    constraints_valid = contraints.validate()
    report_results = contraints.report()
    TEST_LOGGER.info(f"constraints report is: {report_results}")
    # constraints.unbind ??
    # passes_contraints = profile_view.validate(contraints) # fail or ignore missing columns
    #profile_report = profile_view.report(contraints)
    assert constraints_valid

