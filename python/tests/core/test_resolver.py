from whylogs.core import DatasetSchema
from whylogs.core.resolvers import StandardResolver, HistogramCountingTrackingResolver
import whylogs as why
from whylogs.core.metrics.condition_count_metric import Condition, ConditionCountConfig, ConditionCountMetric
from whylogs.core.metrics.condition_count_metric import relation as rel
from whylogs.core.metrics.condition_count_metric import not_relation as not_rel
from whylogs.core.metrics.condition_count_metric import Relation as Rel
from whylogs.core.metrics import StandardMetric
from whylogs.core.datatypes import String, Fractional
import pytest


def test_standard_resolver(pandas_dataframe):
    resolver = StandardResolver()
    schema = DatasetSchema(resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    for _, col in prof_view._columns.items():
        assert len(col._metrics.keys()) > 0


def test_standard_resolver_with_additional_metrics(pandas_dataframe):
    conditions = {
        "not_4": Condition(not_rel(rel(Rel.equal, 4))),
    }

    column_names = ["weight", "legs"]
    why_types = [String(), Fractional()]
    config = ConditionCountConfig(conditions=conditions)

    # applying metrics according to column names
    resolver = StandardResolver()
    resolver.add_standard_metric(metric=StandardMetric.condition_count, column_names=column_names)
    schema = DatasetSchema(default_configs=config, resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    for col_name, col in prof_view._columns.items():
        if col_name in column_names:
            assert "condition_count" in col._metrics.keys()
        else:
            assert "condition_count" not in col._metrics.keys()

    # applying metrics according to data type
    resolver = StandardResolver()
    resolver.add_standard_metric(metric=StandardMetric.condition_count, why_types=why_types)
    schema = DatasetSchema(default_configs=config, resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    for col_name, col in prof_view._columns.items():
        # String and Fractional columns
        if col_name in ["animal", "weight"]:
            assert "condition_count" in col._metrics.keys()
        else:
            assert "condition_count" not in col._metrics.keys()


def test_additional_metrics_invalid_params(pandas_dataframe):

    column_names = ["animal"]
    why_types = [Fractional()]

    resolver = HistogramCountingTrackingResolver()
    with pytest.raises(ValueError) as e:
        resolver.add_standard_metric(metric=StandardMetric.counts, why_types=why_types, column_names=column_names)
        assert e.value.args[0] == "column_names or why_types should be defined, not both."
    with pytest.raises(ValueError) as e:
        resolver.add_standard_metric(metric=StandardMetric.counts)
        assert e.value.args[0] == "Either column names or why types must not be empty."
    with pytest.raises(ValueError) as e:
        resolver.add_standard_metric(metric=ConditionCountMetric)
        assert e.value.args[0] == "Metric must be of StandardMetric type."


def test_additional_metrics_nonexistent(pandas_dataframe):

    column_names = ["nonexistent_column", "another_column"]

    resolver = HistogramCountingTrackingResolver()
    resolver.add_standard_metric(metric=StandardMetric.counts, column_names=column_names)
    schema = DatasetSchema(resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    for col_name, col in prof_view._columns.items():
        assert len(col._metrics.keys()) == 1 and "distribution" in col._metrics.keys()


def test_histogram_resolver(pandas_dataframe):
    column_names = ["animal"]

    resolver = HistogramCountingTrackingResolver()
    resolver.add_standard_metric(metric=StandardMetric.counts, column_names=column_names)
    schema = DatasetSchema(resolvers=resolver)
    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    for col_name, col in prof_view._columns.items():
        if col_name in column_names:
            assert "counts" in col._metrics.keys() and "distribution" in col._metrics.keys()
        else:
            assert "counts" not in col._metrics.keys() and "distribution" in col._metrics.keys()
