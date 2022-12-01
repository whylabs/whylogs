import pytest

import whylogs as why
from whylogs.core.datatypes import AnyType, Fractional, Integral, String
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.condition_count_metric import (
    Condition,
    ConditionCountConfig,
    ConditionCountMetric,
)
from whylogs.core.metrics.condition_count_metric import Relation as Rel
from whylogs.core.metrics.condition_count_metric import relation as rel
from whylogs.core.resolvers import (
    COLUMN_METRICS,
    DeclarativeResolver,
    MetricSpec,
    ResolverSpec,
)
from whylogs.core.schema import DeclarativeSchema

# STANDARD_RESOLVER matches the default DatasetSchema/StandardResolver behavior
STANDARD_RESOLVER = [
    ResolverSpec(
        column_type=Integral,
        metrics=COLUMN_METRICS
        + [
            MetricSpec(StandardMetric.distribution.value),
            MetricSpec(StandardMetric.ints.value),
            MetricSpec(StandardMetric.cardinality.value),
            MetricSpec(StandardMetric.frequent_items.value),
        ],
    ),
    ResolverSpec(
        column_type=Fractional,
        metrics=COLUMN_METRICS
        + [
            MetricSpec(StandardMetric.distribution.value),
            MetricSpec(StandardMetric.cardinality.value),
        ],
    ),
    ResolverSpec(
        column_type=String,
        metrics=COLUMN_METRICS
        + [
            MetricSpec(StandardMetric.unicode_range.value),
            MetricSpec(StandardMetric.distribution.value),
            MetricSpec(StandardMetric.cardinality.value),
            MetricSpec(StandardMetric.frequent_items.value),
        ],
    ),
    ResolverSpec(column_type=AnyType, metrics=COLUMN_METRICS),
]


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


@pytest.mark.parametrize(
    "spec,msg",
    [
        (ResolverSpec(), "DeclarativeSchema: resolver specification must supply name or type"),
        (ResolverSpec(column_type=str), "DeclarativeSchema: resolver specification column type must be a DataType"),
        (
            ResolverSpec(column_name="bruce", metrics=[MetricSpec(int)]),
            "DeclarativeSchema: must supply a Metric subclass to MetricSpec",
        ),
    ],
)
def test_invalid_config(spec, msg) -> None:
    """
    Verify error checking
    """
    with pytest.raises(ValueError, match=msg):
        DeclarativeResolver([spec])


def test_standard_resolver() -> None:
    """
    Verify DeclarativeSchema(STANDARD_RESOLVER) is equivalent to DatasetSchema()
    """

    class UnknownType:
        pass

    data = {"column_1": 3.14, "column_2": "lmno", "column_3": 42, "column_4": UnknownType()}
    standard_results = why.log(row=data).view()

    declarative_standard_schema = DeclarativeSchema(STANDARD_RESOLVER)
    declarative_results = why.log(row=data, schema=declarative_standard_schema).view()

    for column in ["column_1", "column_2", "column_3", "column_4"]:
        standard_metrics = set(standard_results.get_column(column).get_metric_names())
        declarative_metrics = set(declarative_results.get_column(column).get_metric_names())
        assert standard_metrics == declarative_metrics
