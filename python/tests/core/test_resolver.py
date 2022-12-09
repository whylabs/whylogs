import pytest

import whylogs as why
from whylogs.core import DatasetSchema
from whylogs.core.datatypes import Fractional, Integral, String
from whylogs.core.metrics.column_metrics import ColumnCountsMetric
from whylogs.core.metrics.condition_count_metric import (
    Condition,
    ConditionCountConfig,
    ConditionCountMetric,
)
from whylogs.core.metrics.condition_count_metric import Relation as Rel
from whylogs.core.metrics.condition_count_metric import not_relation as not_rel
from whylogs.core.metrics.condition_count_metric import relation as rel
from whylogs.core.metrics.metric_components import IntegralComponent
from whylogs.core.metrics.metrics import DistributionMetric
from whylogs.core.resolvers import (
    EmptyResolver,
    HistogramCountingTrackingResolver,
    MetricSpec,
    ResolverSpec,
    StandardResolver,
)


def test_standard_resolver(pandas_dataframe):
    resolver = StandardResolver()
    schema = DatasetSchema(resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    for _, col in prof_view._columns.items():
        assert len(col._metrics.keys()) > 0


def test_standard_resolver_with_additional_metrics(pandas_dataframe):
    not_4_condition = {
        "not_4": Condition(not_rel(rel(Rel.equal, 4))),
    }

    not_4_3_condition = {
        "not_4.3": Condition(not_rel(rel(Rel.equal, 4.3))),
    }

    resolver = StandardResolver()

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

    resolver.add_resolver_spec(legs_not_4_spec)
    resolver.add_resolver_spec(weights_not_4_2_spec)

    schema = DatasetSchema(resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    for col_name, col in prof_view._columns.items():
        # String and Fractional columns
        if col_name in ["legs", "weight"]:
            assert "condition_count" in col._metrics.keys()
        else:
            assert "condition_count" not in col._metrics.keys()


def test_additional_metrics_invalid_params(pandas_dataframe):

    count_spec = ResolverSpec(
        metrics=[
            MetricSpec(ColumnCountsMetric),
        ],
    )
    resolver = HistogramCountingTrackingResolver()
    resolver.add_resolver_spec(count_spec)
    schema = DatasetSchema(resolvers=resolver)

    with pytest.raises(ValueError) as e:
        prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
        assert e.value.args[0] == "DeclarativeSchema: resolver specification must supply name or type"

    count_spec = ResolverSpec(
        column_name="legs",
        metrics=[
            MetricSpec(IntegralComponent),
        ],
    )

    resolver = HistogramCountingTrackingResolver()
    resolver.add_resolver_spec(count_spec)
    schema = DatasetSchema(resolvers=resolver)

    with pytest.raises(ValueError) as e:
        prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
        assert e.value.args[0] == "DeclarativeSchema: must supply a Metric subclass to MetricSpec"


def test_additional_metrics_nonexistent(pandas_dataframe):

    count_spec = ResolverSpec(
        column_name="nonexistent_columns",
        metrics=[
            MetricSpec(ColumnCountsMetric),
        ],
    )

    resolver = HistogramCountingTrackingResolver()
    resolver.add_resolver_spec(count_spec)
    schema = DatasetSchema(resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    for col_name, col in prof_view._columns.items():
        assert len(col._metrics.keys()) == 1 and "distribution" in col._metrics.keys()


def test_histogram_resolver(pandas_dataframe):
    count_spec = ResolverSpec(
        column_name="animal",
        metrics=[
            MetricSpec(ColumnCountsMetric),
        ],
    )

    resolver = HistogramCountingTrackingResolver()
    resolver.add_resolver_spec(count_spec)
    schema = DatasetSchema(resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    assert "counts" in prof_view._columns["animal"]._metrics.keys()


def test_empty_resolver(pandas_dataframe):
    resolver = EmptyResolver()
    schema = DatasetSchema(resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    assert all([len(x[1]._metrics) == 0 for x in prof_view._columns.items()])
    assert len(prof_view.to_pandas().columns) == 1  # SummaryType.COLUMN
    for why_type in [Fractional, Integral, String]:

        count_resolver_spec = ResolverSpec(column_type=why_type, metrics=[MetricSpec(metric=ColumnCountsMetric)])

        if why_type == Fractional:
            distribution_spec = ResolverSpec(column_type=why_type, metrics=[MetricSpec(metric=DistributionMetric)])
            resolver.add_resolver_spec(distribution_spec)

        resolver.add_resolver_spec(count_resolver_spec)
    schema = DatasetSchema(resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    metrics = list(prof_view.to_pandas().columns)
    assert all([x.startswith(("counts", "type", "distribution")) for x in metrics])
