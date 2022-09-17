import re
from typing import Any, Dict, List

import pandas as pd
import pytest

from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.datatypes import DataType
from whylogs.core.metric_getters import MetricGetter, ProfileGetter
from whylogs.core.metrics import DistributionMetric, Metric, MetricConfig
from whylogs.core.metrics.condition_count_metric import (
    Condition,
    ConditionCountConfig,
    ConditionCountMetric,
)
from whylogs.core.metrics.metric_components import IntegralComponent
from whylogs.core.metrics.metrics import OperationResult
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.relations import Relation as Rel
from whylogs.core.relations import and_relations as and_rel
from whylogs.core.relations import not_relation as not_rel
from whylogs.core.relations import or_relations as or_rel
from whylogs.core.relations import relation as rel
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema


def test_condition_count_metric() -> None:
    conditions = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
        "digit": Condition(rel(Rel.match, "[0-9]+")),
    }
    metric = ConditionCountMetric(conditions, IntegralComponent(0))
    strings = ["abc", "123", "kwatz", "314159", "abc123"]
    metric.columnar_update(PreprocessedColumn.apply(strings))
    summary = metric.to_summary_dict(None)

    assert set(summary.keys()) == {"total", "alpha", "digit"}
    assert summary["total"] == len(strings)
    assert summary["alpha"] == 3  # "abc123" matches since it's not fullmatch
    assert summary["digit"] == 2


def test_throw_on_failure() -> None:
    conditions = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+"), throw_on_failure=True),
        "beta": Condition(rel(Rel.less, "blah"), throw_on_failure=True),
    }
    metric = ConditionCountMetric(conditions, IntegralComponent(0))
    strings = ["abc", "123", "kwatz", "314159", "abc123"]
    with pytest.raises(ValueError):
        metric.columnar_update(PreprocessedColumn.apply(strings))
    strings = ["b", "bl", "bla"]
    assert metric.columnar_update(PreprocessedColumn.apply(strings)) == OperationResult(0, len(strings))


def test_condition_count_merge() -> None:
    conditions = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
        "digit": Condition(rel(Rel.match, "[0-9]+")),
    }
    metric1 = ConditionCountMetric(conditions, IntegralComponent(0))
    metric2 = ConditionCountMetric(conditions, IntegralComponent(0))
    strings = ["abc", "123", "kwatz", "314159", "abc123"]
    metric1.columnar_update(PreprocessedColumn.apply(strings))
    metric2.columnar_update(PreprocessedColumn.apply(strings))
    metric = metric1.merge(metric2)
    summary = metric.to_summary_dict(None)

    assert set(summary.keys()) == {"total", "alpha", "digit"}
    assert summary["total"] == 2 * len(strings)
    assert summary["alpha"] == 2 * 3  # "abc123" matches since it's not fullmatch
    assert summary["digit"] == 2 * 2


def test_condition_count_bad_merge() -> None:
    conditions1 = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
        "digit": Condition(rel(Rel.match, "[0-9]+")),
    }
    metric1 = ConditionCountMetric(conditions1, IntegralComponent(0))
    conditions2 = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
        "digit": Condition(rel(Rel.match, "[0-9]+")),
        "alnum": Condition(rel(Rel.match, "[a-zA-Z0-9]+")),
    }
    metric2 = ConditionCountMetric(conditions2, IntegralComponent(0))
    strings = ["abc", "123", "kwatz", "314159", "abc123"]
    metric1.columnar_update(PreprocessedColumn.apply(strings))
    metric2.columnar_update(PreprocessedColumn.apply(strings))
    metric = metric1.merge(metric2)
    summary = metric.to_summary_dict(None)

    assert set(summary.keys()) == {"total", "alpha", "digit"}
    assert summary["total"] == len(strings)
    assert summary["alpha"] == 3  # "abc123" matches since it's not fullmatch
    assert summary["digit"] == 2


def test_add_conditions_to_metric() -> None:
    conditions = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
    }
    metric = ConditionCountMetric(conditions, IntegralComponent(0))
    strings = ["abc", "123", "kwatz", "314159", "abc123"]
    metric.columnar_update(PreprocessedColumn.apply(strings))
    metric.add_conditions({"digit": Condition(rel(Rel.match, "[0-9]+"))})
    metric.columnar_update(PreprocessedColumn.apply(strings))
    summary = metric.to_summary_dict(None)

    assert set(summary.keys()) == {"total", "alpha", "digit"}
    assert summary["total"] == 2 * len(strings)
    assert summary["alpha"] == 2 * 3  # "abc123" matches since it's not fullmatch
    assert summary["digit"] == 2


def test_condition_predicates() -> None:
    def even(x: Any, dummy: Any) -> bool:
        return x % 2 == 0

    conditions = {
        "match": Condition(rel(Rel.match, "[a-zA-Z]+")),
        "fullmatch": Condition(rel(Rel.fullmatch, "[a-zA-Z]+")),
        "equal_str": Condition(rel(Rel.equal, "42")),
        "equal_int": Condition(rel(Rel.equal, 42)),
        "equal_flt": Condition(rel(Rel.equal, 42.1)),
        "equal_flt42": Condition(rel(Rel.equal, 42.0)),
        "less": Condition(rel(Rel.less, 42)),
        "leq": Condition(rel(Rel.leq, 42)),
        "greater": Condition(rel(Rel.greater, 42)),
        "geq": Condition(rel(Rel.geq, 42)),
        "neq": Condition(rel(Rel.neq, 42)),
        "udf": Condition((even, lambda: 0)),
    }
    config = ConditionCountConfig(conditions=conditions)
    metric = ConditionCountMetric.zero(config)
    data = ["abc", "abc123", "42", 41, 42, 42.0, 42.1, 43]
    metric.columnar_update(PreprocessedColumn.apply(data))
    summary = metric.to_summary_dict(None)

    assert summary["total"] == len(data)
    assert summary["match"] == 2
    assert summary["fullmatch"] == 1
    assert summary["equal_str"] == 1
    assert summary["equal_int"] == 2
    assert summary["equal_flt"] == 1
    assert summary["equal_flt42"] == 2
    assert summary["less"] == 1
    assert summary["leq"] == 3
    assert summary["greater"] == 2
    assert summary["geq"] == 4
    assert summary["neq"] == 6
    assert summary["udf"] == 2


def test_condition_bool_ops() -> None:
    conditions = {
        "between": Condition(and_rel(rel(Rel.greater, 40), rel(Rel.less, 44))),
        "outside": Condition(or_rel(rel(Rel.less, 40), rel(Rel.greater, 44))),
        "not_alpha": Condition(not_rel(rel(Rel.match, "[a-zA-Z]+"))),
    }
    config = ConditionCountConfig(conditions=conditions)
    metric = ConditionCountMetric.zero(config)
    data = ["abc", "123", 10, 42, 50]
    metric.columnar_update(PreprocessedColumn.apply(data))
    summary = metric.to_summary_dict(None)

    assert summary["total"] == len(data)
    assert summary["between"] == 1
    assert summary["outside"] == 2
    assert summary["not_alpha"] == 1  # numbers cause type error and don't count


def test_bad_condition_name() -> None:
    conditions = {
        "total": Condition(rel(Rel.match, "")),
    }
    with pytest.raises(ValueError):
        ConditionCountMetric(conditions, IntegralComponent(0))

    metric = ConditionCountMetric({}, IntegralComponent(0))
    with pytest.raises(ValueError):
        metric.add_conditions({"total": re.compile("")})


def test_condition_count_in_profile() -> None:
    class TestResolver(Resolver):
        def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
            return {"condition_count": ConditionCountMetric.zero(column_schema.cfg)}

    conditions = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
        "digit": Condition(rel(Rel.match, "[0-9]+")),
    }
    config = ConditionCountConfig(conditions=conditions)
    resolver = TestResolver()
    schema = DatasetSchema(default_configs=config, resolvers=resolver)

    row = {"col1": ["abc", "123"]}
    prof = DatasetProfile(schema)
    prof.track(row=row)
    prof1_view = prof.view()
    prof1_view.write("/tmp/test_condition_count_metric_in_profile")
    prof2_view = DatasetProfile.read("/tmp/test_condition_count_metric_in_profile")
    prof1_cols = prof1_view.get_columns()
    prof2_cols = prof2_view.get_columns()

    assert prof1_cols.keys() == prof2_cols.keys()
    for col_name in prof1_cols.keys():
        col1_prof = prof1_cols[col_name]
        col2_prof = prof2_cols[col_name]
        assert (col1_prof is not None) == (col2_prof is not None)
        if col1_prof:
            assert col1_prof._metrics.keys() == col2_prof._metrics.keys()
            assert col1_prof.to_summary_dict() == col2_prof.to_summary_dict()
            assert {
                "condition_count/total",
                "condition_count/alpha",
                "condition_count/digit",
            } <= col1_prof.to_summary_dict().keys()


def test_condition_count_in_column_profile() -> None:
    conditions = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
        "digit": Condition(rel(Rel.match, "[0-9]+")),
    }
    config = ConditionCountConfig(conditions=conditions)
    metric = ConditionCountMetric.zero(config)

    row = {"col1": ["abc", "123"]}
    frame = pd.DataFrame(data=row)
    prof = DatasetProfile()
    # Column names must be known in the profile, and that doesn't
    # happen until some data has been logged
    prof.track(pandas=frame)

    prof._columns["col1"].add_metric(metric)
    prof.track(pandas=frame)
    prof_view = prof.view()

    summary = prof_view.get_column("col1").to_summary_dict()
    assert summary["condition_count/total"] > 0
    assert summary["condition_count/alpha"] > 0
    assert summary["condition_count/digit"] > 0


def test_condition_count_in_dataset_profile() -> None:
    conditions = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
        "digit": Condition(rel(Rel.match, "[0-9]+")),
    }
    config = ConditionCountConfig(conditions=conditions)
    metric = ConditionCountMetric.zero(config)

    row = {"col1": ["abc", "123"]}
    frame = pd.DataFrame(data=row)
    prof = DatasetProfile()
    prof.track(pandas=frame)

    prof.add_metric("col1", metric)
    prof.track(pandas=frame)
    prof_view = prof.view()

    summary = prof_view.get_column("col1").to_summary_dict()
    assert summary["condition_count/total"] > 0
    assert summary["condition_count/alpha"] > 0
    assert summary["condition_count/digit"] > 0


def _build_profile(data: List[int]) -> DatasetProfile:
    """build up a "reference profile" to compare against"""
    row = {"col1": data}
    frame = pd.DataFrame(data=row)
    prof = DatasetProfile()
    prof.track(pandas=frame)  # track once to discover columns
    prof.add_metric("col1", DistributionMetric.zero(MetricConfig()))
    prof.track(pandas=frame)  # track again to populate the metric
    return prof


def test_profile_getter() -> None:
    data = [1, 2, 3, 4, 5]
    prof = _build_profile(data)
    conditions = {
        "above_min": Condition(rel(Rel.greater, ProfileGetter(prof, "col1", "distribution/min"))),
    }  # compare each logged value against profile's min
    config = ConditionCountConfig(conditions=conditions)
    metric = ConditionCountMetric.zero(config)
    metric.columnar_update(PreprocessedColumn.apply(data))
    summary = metric.to_summary_dict(None)
    assert summary["total"] == len(data)
    assert summary["above_min"] == len(data) - 1


def test_metric_getter() -> None:
    data = [1, 2, 3, 4, 5]
    dist_metric = DistributionMetric.zero(MetricConfig())
    dist_metric.columnar_update(PreprocessedColumn.apply(data))

    conditions = {
        "above_min": Condition(rel(Rel.greater, MetricGetter(dist_metric, "min"))),
    }  # compare each logged value against dist_metric's min
    config = ConditionCountConfig(conditions=conditions)
    cond_metric = ConditionCountMetric.zero(config)
    cond_metric.columnar_update(PreprocessedColumn.apply(data))
    summary = cond_metric.to_summary_dict(None)
    assert summary["total"] == len(data)
    assert summary["above_min"] == len(data) - 1
