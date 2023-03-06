import pytest

import whylogs as why
from whylogs.core import DatasetSchema
from whylogs.core.datatypes import Fractional, String
from whylogs.core.metrics import MetricConfig, StandardMetric
from whylogs.core.metrics.column_metrics import ColumnCountsMetric, TypeCountersMetric
from whylogs.core.metrics.condition_count_metric import (
    Condition,
    ConditionCountConfig,
    ConditionCountMetric,
)
from whylogs.core.metrics.condition_count_metric import Relation as Rel
from whylogs.core.metrics.condition_count_metric import relation as rel
from whylogs.core.resolvers import (
    HISTOGRAM_COUNTING_TRACKING_RESOLVER,
    LIMITED_TRACKING_RESOLVER,
    STANDARD_RESOLVER,
    HistogramCountingTrackingResolver,
    LimitedTrackingResolver,
    MetricSpec,
    ResolverSpec,
    StandardResolver,
)
from whylogs.core.schema import DeclarativeSchema


def test_declarative_schema() -> None:
    """
    Exercise basic schema -- column matching by name and type, per-column
    metric configuration
    """

    schema = DeclarativeSchema(
        [
            ResolverSpec(
                column_name="column_1",
                metrics=[
                    MetricSpec(StandardMetric.distribution.value),
                    MetricSpec(
                        ConditionCountMetric,
                        ConditionCountConfig(
                            conditions={
                                "below 42": Condition(lambda x: x < 42),
                                "above 42": Condition(lambda x: x > 42),
                            }
                        ),
                    ),
                ],
            ),
            ResolverSpec(
                column_type=String,
                metrics=[
                    MetricSpec(StandardMetric.frequent_items.value),
                    MetricSpec(
                        ConditionCountMetric,
                        ConditionCountConfig(
                            conditions={
                                "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
                                "digit": Condition(rel(Rel.match, "[0-9]+")),
                            }
                        ),
                    ),
                ],
            ),
        ]
    )

    data = {"column_1": 3.14, "column_2": "lmno"}
    results = why.log(row=data, schema=schema).view()
    col1_metrics = set(results.get_column("column_1").get_metric_names())
    assert col1_metrics == {"distribution", "condition_count"}
    col1_conditions = set(results.get_column("column_1").get_metric("condition_count").conditions.keys())
    assert col1_conditions == {"below 42", "above 42"}

    col2_metrics = set(results.get_column("column_2").get_metric_names())
    assert col2_metrics == {"frequent_items", "condition_count"}
    col2_conditions = set(results.get_column("column_2").get_metric("condition_count").conditions.keys())
    assert col2_conditions == {"alpha", "digit"}


def test_declarative_schema_with_additional_resolvers(pandas_dataframe):
    not_4_condition = {
        "not_4": Condition(lambda x: x != 4),
    }

    not_4_3_condition = {
        "not_4.3": Condition(lambda x: x != 4.3),
    }

    legs_not_4_spec = ResolverSpec(
        column_name="legs",
        metrics=[
            MetricSpec(
                ConditionCountMetric,
                ConditionCountConfig(conditions=not_4_condition),
            ),
        ],
    )

    weights_not_4_2_spec = ResolverSpec(
        column_type=Fractional,
        metrics=[
            MetricSpec(
                ConditionCountMetric,
                ConditionCountConfig(conditions=not_4_3_condition),
            ),
        ],
    )

    schema = DeclarativeSchema(STANDARD_RESOLVER)
    schema.add_resolver(legs_not_4_spec)
    schema.add_resolver(weights_not_4_2_spec)

    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    colset = prof_view.to_pandas().columns

    num_count_components = len(ColumnCountsMetric.zero().to_summary_dict().keys())
    num_type_components = len(TypeCountersMetric.zero().to_summary_dict().keys())
    num_dist_components = len(StandardMetric.distribution.value.zero().to_summary_dict().keys())
    num_freq_items_components = len(StandardMetric.frequent_items.value.zero().to_summary_dict().keys())
    num_card_components = len(StandardMetric.cardinality.value.zero().to_summary_dict().keys())
    num_int_components = len(StandardMetric.ints.value.zero().to_summary_dict().keys())
    num_cond_components = 3  # total, not_4, not_4.3

    expected_column_count = (
        num_count_components
        + num_type_components
        + num_dist_components
        + num_freq_items_components
        + num_card_components
        + num_int_components
        + num_cond_components
    )

    # - 1 for 'type' column, which is from Pandas, not whylogs
    assert len(colset) - 1 == expected_column_count
    assert {"condition_count/not_4", "condition_count/not_4.3", "condition_count/total"}.issubset(colset)


def test_additional_metrics_nonexistent(pandas_dataframe):
    count_spec = ResolverSpec(
        column_name="nonexistent_columns",
        metrics=[
            MetricSpec(ColumnCountsMetric),
        ],
    )
    schema = DeclarativeSchema(HISTOGRAM_COUNTING_TRACKING_RESOLVER)
    schema.add_resolver(count_spec)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    col_list = list(prof_view.to_pandas().columns)
    assert all([col.startswith(("type", "distribution")) for col in col_list])


@pytest.mark.parametrize("fi_disabled,unicode_enabled", [(False, False), (False, True), (True, False), (True, True)])
def test_enabled_metrics(fi_disabled: bool, unicode_enabled: bool) -> None:
    config = MetricConfig(fi_disabled=fi_disabled, track_unicode_ranges=unicode_enabled)
    schema = DeclarativeSchema(STANDARD_RESOLVER, default_config=config)
    data = {"col1": "foo"}
    results = why.log(row=data, schema=schema).view()
    col1_metrics = set(results.get_column("col1").get_metric_names())
    assert fi_disabled == ("frequent_items" not in col1_metrics)
    assert unicode_enabled == ("unicode_range" in col1_metrics)


def test_invalid_config() -> None:
    """
    Verify error checking
    """
    with pytest.raises(ValueError) as e:
        ResolverSpec()
        assert e.value.args[0] == "ResolverSpec: resolver specification must supply name or type"
    with pytest.raises(ValueError) as e:
        ResolverSpec(column_type=str)
        assert e.value.args[0] == "ResolverSpec: resolver specification column type must be a DataType"
    with pytest.raises(ValueError) as e:
        ResolverSpec(column_name="bruce", metrics=[MetricSpec(int)])
        assert e.value.args[0] == "MetricSpec: must supply a Metric subclass to MetricSpec"


def test_resolvers() -> None:
    """
    Verify DeclarativeSchema(RESOLVER) is equivalent to DatasetSchema()
    for different RESOLVERS (Standard, Limited Tracking, HistogramCounting)
    """
    params = [
        (StandardResolver(), STANDARD_RESOLVER),
        (LimitedTrackingResolver(), LIMITED_TRACKING_RESOLVER),
        (HistogramCountingTrackingResolver(), HISTOGRAM_COUNTING_TRACKING_RESOLVER),
    ]

    class UnknownType:
        pass

    data = {"column_1": 3.14, "column_2": "lmno", "column_3": 42, "column_4": UnknownType()}
    for reference_resolver, declarative_resolver in params:
        reference_results = why.log(row=data, schema=DatasetSchema(resolvers=reference_resolver)).view()
        declarative_standard_schema = DeclarativeSchema(declarative_resolver)
        declarative_results = why.log(row=data, schema=declarative_standard_schema).view()

        for column in ["column_1", "column_2", "column_3", "column_4"]:
            reference_metrics = set(reference_results.get_column(column).get_metric_names())
            declarative_metrics = set(declarative_results.get_column(column).get_metric_names())
            assert reference_metrics == declarative_metrics
