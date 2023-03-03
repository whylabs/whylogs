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
from whylogs.core.metrics.condition_count_metric import Relation as Rel
from whylogs.core.metrics.condition_count_metric import relation as rel
from whylogs.core.metrics.metric_components import IntegralComponent
from whylogs.core.metrics.metrics import OperationResult
from whylogs.core.metrics.unicode_range import UnicodeRangeMetric
from whylogs.core.predicate_parser import _tokenize, parse_predicate
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.relations import Not, Predicate, Require
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema

X = Predicate()


def test_condition_count_metric() -> None:
    conditions = {
        "alpha": Condition(X.matches("[a-zA-Z]+")),
        "digit": Condition(X.matches("[0-9]+")),
        "kwatz": Condition(X.equals("kwatz")),
    }
    metric = ConditionCountMetric(conditions, IntegralComponent(0))
    strings = ["abc", "123", "kwatz", "314159", "abc123"]
    metric.columnar_update(PreprocessedColumn.apply(strings))
    summary = metric.to_summary_dict(None)

    assert set(summary.keys()) == {"total", "alpha", "digit", "kwatz"}
    assert summary["total"] == len(strings)
    assert summary["alpha"] == 3  # "abc123" matches since it's not fullmatch
    assert summary["digit"] == 2
    assert summary["kwatz"] == 1


def test_throw_on_failure() -> None:
    conditions = {
        "alpha": Condition(X.matches("[a-zA-Z]+"), throw_on_failure=True),
        "beta": Condition(X.less_than("blah"), throw_on_failure=True),
    }
    metric = ConditionCountMetric(conditions, IntegralComponent(0))
    strings = ["abc", "123", "kwatz", "314159", "abc123"]
    with pytest.raises(ValueError):
        metric.columnar_update(PreprocessedColumn.apply(strings))
    strings = ["b", "bl", "bla"]
    assert metric.columnar_update(PreprocessedColumn.apply(strings)) == OperationResult(0, len(strings))


action_1_count = 0
action_2_count = 0


def action_1(val_name: str, cond_name: str, value: Any) -> None:
    global action_1_count
    action_1_count += 1


def action_2(val_name: str, cond_name: str, value: Any) -> None:
    global action_2_count
    action_2_count += 1


def test_actions() -> None:
    conditions = {
        "alpha": Condition(rel(Rel.match, "[a-zA-Z]+"), actions=[action_1]),
        "digit": Condition(rel(Rel.match, "[0-9]+"), actions=[action_1, action_2]),
    }
    metric = ConditionCountMetric(conditions, IntegralComponent(0))
    strings = ["abc", "123", "kwatz", "314159", "abc123"]
    metric.columnar_update(PreprocessedColumn.apply(strings))

    assert action_1_count == 5  # 3 don't start with digits, 2 don't start with digits
    assert action_2_count == 3  # 3 don't start with digits


def test_condition_count_merge() -> None:
    conditions = {
        "alpha": Condition(X.matches("[a-zA-Z]+")),
        "digit": Condition(X.matches("[0-9]+")),
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
        "alpha": Condition(X.matches("[a-zA-Z]+")),
        "digit": Condition(X.matches("[0-9]+")),
    }
    metric1 = ConditionCountMetric(conditions1, IntegralComponent(0))
    conditions2 = {
        "alpha": Condition(X.matches("[a-zA-Z]+")),
        "digit": Condition(X.matches("[0-9]+")),
        "alnum": Condition(X.matches("[a-zA-Z0-9]+")),
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
        "alpha": Condition(X.matches("[a-zA-Z]+")),
    }
    metric = ConditionCountMetric(conditions, IntegralComponent(0))
    strings = ["abc", "123", "kwatz", "314159", "abc123"]
    metric.columnar_update(PreprocessedColumn.apply(strings))
    metric.add_conditions({"digit": Condition(X.matches("[0-9]+"))})
    metric.columnar_update(PreprocessedColumn.apply(strings))
    summary = metric.to_summary_dict(None)

    assert set(summary.keys()) == {"total", "alpha", "digit"}
    assert summary["total"] == 2 * len(strings)
    assert summary["alpha"] == 2 * 3  # "abc123" matches since it's not fullmatch
    assert summary["digit"] == 2


def test_condition_predicates() -> None:
    def even(x: Any) -> bool:
        return x % 2 == 0

    conditions = {
        "match": Condition(X.matches("[a-zA-Z]+")),
        "fullmatch": Condition(X.fullmatch("[a-zA-Z]+")),
        "equal_str": Condition(X.equals("42")),
        "equal_int": Condition(X.equals(42)),
        "equal_flt": Condition(X.equals(42.1)),
        "equal_flt42": Condition(X.equals(42.0)),
        "less": Condition(X.less_than(42)),
        "leq": Condition(X.less_or_equals(42)),
        "greater": Condition(X.greater_than(42)),
        "geq": Condition(X.greater_or_equals(42)),
        "neq": Condition(X.not_equal(42)),
        "udf": Condition(X.is_(even)),
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
        "between": Condition(X.greater_than(40).and_(X.less_than(44))),
        "outside": Condition(X.less_than(40).or_(X.greater_than(44))),
        "not_alpha": Condition(X.not_.matches("[a-zA-Z]+")),
        "not_alpha2": Condition(Not(X.matches("[a-zA-Z]+"))),
    }
    config = ConditionCountConfig(conditions=conditions)
    metric = ConditionCountMetric.zero(config)
    data = ["abc", "123", 10, 42, 50]
    metric.columnar_update(PreprocessedColumn.apply(data))
    summary = metric.to_summary_dict(None)

    assert summary["total"] == len(data)
    assert summary["between"] == 1
    assert summary["outside"] == 2
    assert summary["not_alpha"] == 4
    assert summary["not_alpha2"] == 4


def test_bad_condition_name() -> None:
    conditions = {
        "total": Condition(X.matches("")),
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
        "alpha": Condition(X.matches("[a-zA-Z]+")),
        "digit": Condition(X.matches("[0-9]+")),
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
        "alpha": Condition(X.matches("[a-zA-Z]+")),
        "digit": Condition(X.matches("[0-9]+")),
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
        "alpha": Condition(X.matches("[a-zA-Z]+")),
        "digit": Condition(X.matches("[0-9]+")),
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


def _build_profile(data: List[int], col_name: str = "col1") -> DatasetProfile:
    """build up a "reference profile" to compare against"""
    row = {col_name: data}
    frame = pd.DataFrame(data=row)
    prof = DatasetProfile()
    prof.track(pandas=frame)  # track once to discover columns
    prof.add_metric(col_name, DistributionMetric.zero(MetricConfig()))
    prof.add_metric(col_name, UnicodeRangeMetric.zero())
    prof.track(pandas=frame)  # track again to populate the metric
    return prof


def test_profile_view_getter() -> None:
    data = [1, 2, 3, 4, 5]
    prof = _build_profile(data)
    conditions = {
        "above_min": Condition(X.greater_than(ProfileGetter(prof.view(), "col1", "distribution/min"))),
    }  # compare each logged value against profile's min
    config = ConditionCountConfig(conditions=conditions)
    metric = ConditionCountMetric.zero(config)
    metric.columnar_update(PreprocessedColumn.apply(data))
    summary = metric.to_summary_dict(None)
    assert summary["total"] == len(data)
    assert summary["above_min"] == len(data) - 1


def test_profile_getter() -> None:
    data = [1, 2, 3, 4, 5]
    prof = _build_profile(data)
    conditions = {
        "above_min": Condition(X.greater_than(ProfileGetter(prof, "col1", "distribution/min"))),
    }  # compare each logged value against profile's min
    config = ConditionCountConfig(conditions=conditions)
    metric = ConditionCountMetric.zero(config)
    metric.columnar_update(PreprocessedColumn.apply(data))
    summary = metric.to_summary_dict(None)
    assert summary["total"] == len(data)
    assert summary["above_min"] == len(data) - 1

    assert conditions["above_min"].relation.serialize() == "> x :col1:distribution/min"
    assert parse_predicate("> x :col1:distribution/min", profile=prof).serialize() == "> x :col1:distribution/min"


def test_metric_getter() -> None:
    data = [1, 2, 3, 4, 5]
    dist_metric = DistributionMetric.zero(MetricConfig())
    dist_metric.columnar_update(PreprocessedColumn.apply(data))

    conditions = {
        "above_min": Condition(X.greater_than(MetricGetter(dist_metric, "min"))),
    }  # compare each logged value against dist_metric's min
    config = ConditionCountConfig(conditions=conditions)
    cond_metric = ConditionCountMetric.zero(config)
    cond_metric.columnar_update(PreprocessedColumn.apply(data))
    summary = cond_metric.to_summary_dict(None)
    assert summary["total"] == len(data)
    assert summary["above_min"] == len(data) - 1

    assert conditions["above_min"].relation.serialize() == "> x ::distribution/min"
    assert parse_predicate("> x ::distribution/min", metric=dist_metric).serialize() == "> x ::distribution/min"


@pytest.mark.parametrize(
    "input,expected",
    [
        ('~ x "abc def"', ["~", "x", '"abc def"']),
        (r'~ x "abc\"def"', ["~", "x", r'"abc"def"']),
        (r'  ==   x  " \"\" \" "', ["==", "x", r'" "" " "']),
        (r'  ==   x  "\"\" \""', ["==", "x", r'""" ""']),
        (r'"\\"', [r'"\"']),
        (r'"\\\"" "\\ \" \\\"\\" "\foo"', [r'"\""', r'"\ " \"\"', r'"\foo"']),
        (r'  ==   x  "\x"', ["==", "x", r'"\x"']),
        (r'  ==   x  "\"\x"', ["==", "x", r'""\x"']),
        (r'  ==   x  "\\"', ["==", "x", r'"\"']),
        (r'  ==   x  "\\\""', ["==", "x", r'"\""']),
        ("::distribution/n", ["::distribution/n"]),
        (":col1:distribution/n", [":col1:distribution/n"]),
        ("::multimetric/subname:distribution/n", ["::multimetric/subname:distribution/n"]),
        (":col1:multimetric/subname:distribution/n", [":col1:multimetric/subname:distribution/n"]),
        (
            ":foo bar:distribution/n :col name:multimetric/subname:distribution/n",
            [":foo bar:distribution/n", ":col name:multimetric/subname:distribution/n"],
        ),
        (
            r":annoying\:name:distribution/n  :annoying\: name:multimetric/subname:distribution/n",
            [":annoying:name:distribution/n", ":annoying: name:multimetric/subname:distribution/n"],
        ),
        (r":\\::foo/bar", [r":\::foo/bar"]),
        (r":foo\bar:distribution/component", [r":foo\bar:distribution/component"]),
    ],
)
def test_expression_tokenizer(input: str, expected: List[str]) -> None:
    assert _tokenize(input) == expected


@pytest.mark.parametrize(
    "predicate,serialized",
    [
        (X.matches('abc"def'), r'~ x "abc\"def"'),
        (X.matches("abc def"), '~ x "abc def"'),
        (X.matches("[a-zA-Z]+"), '~ x "[a-zA-Z]+"'),
        (X.fullmatch("[a-zA-Z]+"), '~= x "[a-zA-Z]+"'),
        (X.equals("42"), '== x "42"'),
        (X.equals(42), "== x 42"),
        (X.equals(42.1), "== x 42.1"),
        (X.equals(42.0), "== x 42.0"),
        (X.less_than(42), "< x 42"),
        (X.less_or_equals(42), "<= x 42"),
        (X.greater_than(-42), "> x -42"),
        (X.greater_than(42), "> x 42"),
        (X.greater_or_equals(42), ">= x 42"),
        (X.not_equal(42), "!= x 42"),
        # (X.is_(even)),
        (X.greater_than(40).and_(X.less_than(44)), "and > x 40 < x 44"),
        (X.less_than(40).or_(X.greater_than(44)), "or < x 40 > x 44"),
        (X.not_.matches("[a-zA-Z]+"), 'not ~ x "[a-zA-Z]+"'),
        (Not(X.matches("[a-zA-Z]+")), 'not ~ x "[a-zA-Z]+"'),
        #
        (Require("mean").matches("[a-zA-Z]+"), '~ mean "[a-zA-Z]+"'),
        (Require("mean").fullmatch("[a-zA-Z]+"), '~= mean "[a-zA-Z]+"'),
        (Require("mean").equals("42"), '== mean "42"'),
        (Require("mean").equals(42), "== mean 42"),
        (Require("mean").equals(42.1), "== mean 42.1"),
        (Require("mean").equals(42.0), "== mean 42.0"),
        (Require("mean").less_than(42), "< mean 42"),
        (Require("mean").less_or_equals(42), "<= mean 42"),
        (Require("mean").greater_than(-42), "> mean -42"),
        (Require("mean").greater_than(42), "> mean 42"),
        (Require("mean").greater_or_equals(42), ">= mean 42"),
        (Require("mean").not_equal(42), "!= mean 42"),
        # (Require().is_(even)),
        (Require("mean").greater_than(40).and_(Require("max").less_than(44)), "and > mean 40 < max 44"),
        (Require("mean").less_than(40).or_(Require("min").greater_than(44)), "or < mean 40 > min 44"),
        (Require("mean").not_.matches("[a-zA-Z]+"), 'not ~ mean "[a-zA-Z]+"'),
        (Not(Require("mean").matches("[a-zA-Z]+")), 'not ~ mean "[a-zA-Z]+"'),
    ],
)
def test_serialization(predicate: X, serialized: str) -> None:
    assert predicate.serialize() == serialized


@pytest.mark.parametrize(
    "predicate",
    [
        (X.matches('abc"def')),
        (X.matches("abc def")),
        (X.matches("[a-zA-Z]+")),
        (X.fullmatch("[a-zA-Z]+")),
        (X.equals("42")),
        (X.equals(42)),
        (X.equals(42.1)),
        (X.equals(42.0)),
        (X.less_than(42)),
        (X.less_or_equals(42)),
        (X.greater_than(42)),
        (X.greater_than(-42)),
        (X.greater_or_equals(42)),
        (X.not_equal(42)),
        # (X.is_(even)),
        (X.greater_than(40).and_(X.less_than(44))),
        (X.less_than(40).or_(X.greater_than(44))),
        (X.not_.matches("[a-zA-Z]+")),
        (Not(X.matches("[a-zA-Z]+"))),
        #
        (Require("mean").matches("[a-zA-Z]+")),
        (Require("mean").fullmatch("[a-zA-Z]+")),
        (Require("mean").equals("42")),
        (Require("mean").equals(42)),
        (Require("mean").equals(42.1)),
        (Require("mean").equals(42.0)),
        (Require("mean").less_than(42)),
        (Require("mean").less_or_equals(42)),
        (Require("mean").greater_than(42)),
        (Require("mean").greater_than(-42)),
        (Require("mean").greater_or_equals(42)),
        (Require("mean").not_equal(42)),
        # (Require().is_(even)),
        (Require("mean").greater_than(40).and_(Require("max").less_than(44))),
        (Require("mean").less_than(40).or_(Require("min").greater_than(44))),
        (Require("mean").not_.matches("[a-zA-Z]+")),
        (Not(Require("mean").matches("[a-zA-Z]+"))),
    ],
)
def test_deserialization(predicate: X) -> None:
    serialized = predicate.serialize()
    assert parse_predicate(serialized).serialize() == serialized


@pytest.mark.parametrize(
    "col_name,expression",
    [
        ("col1", "== n 5"),
        ("col1", "== n :col1:distribution/n"),
        ("col1", "== n ::distribution/n"),
        ("annoying name", "== n 5"),
        ("annoying name", "== n :annoying name:distribution/n"),
        ("annoying:name", "== n 5"),
        ("annoying:name", r"== n :annoying\:name:distribution/n"),
        (":very : annoying: name:", r"== n :\:very \: annoying\: name\::distribution/n"),
        (":very : annoying: name:", r"== min :\:very \: annoying\: name\::unicode_range/string_length:distribution/n"),
        (":", r"== n :\::distribution/n"),
    ],
)
def test_metric_getter_deserialization(col_name: str, expression: str) -> None:
    data = [0, 2, 3, 4, 5]
    prof = _build_profile(data, col_name)
    metric = prof.view().get_column(col_name).get_metric("distribution")
    assert metric

    predicate = parse_predicate(expression, profile=prof, metric=metric)
    assert predicate(metric)
    assert predicate.serialize() == expression
