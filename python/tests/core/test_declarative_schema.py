import pytest

import whylogs as why
from whylogs.core.datatypes import String
from whylogs.core.declarative_schema import DeclarativeResolver, DeclarativeSchema, MetricSpec, ResolverSpec
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.condition_count_metric import Condition, ConditionCountConfig, ConditionCountMetric
from whylogs.core.metrics.condition_count_metric import Relation as Rel
from whylogs.core.metrics.condition_count_metric import relation as rel


def test_declarative_schema() -> None:
    
    schema = DeclarativeSchema(
        [
            ResolverSpec(
                column_name = "column_1",
                metrics = [
                    MetricSpec(StandardMetric.distribution.value),
                    MetricSpec(
                        ConditionCountMetric,
                        ConditionCountConfig(
                            conditions={
                                "below 42": Condition(lambda x: x < 42),
                                "above 42": Condition(lambda x: x > 42),
                            }
                        )
                    ),
                ]
            ),
            ResolverSpec(
                column_type = String,
                metrics = [
                    MetricSpec(StandardMetric.frequent_items.value),
                    MetricSpec(
                        ConditionCountMetric,
                        ConditionCountConfig(
                            conditions={
                                "alpha": Condition(rel(Rel.match, "[a-zA-Z]+")),
                                "digit": Condition(rel(Rel.match, "[0-9]+")),
                            }
                        )
                    ),                        
                ]
            ),
        ]
    )

    data = {"column_1": 3.14, "column_2": "lmno"}
    results = why.log(row=data, schema = schema).view()
    col1_metrics = set(results.get_column("column_1").get_metric_names())
    assert col1_metrics == {"distribution", "condition_count"}
    col1_conditions = set(results.get_column("column_1").get_metric("condition_count").conditions.keys())
    assert col1_conditions == {"below 42", "above 42"}

    col2_metrics = set(results.get_column("column_2").get_metric_names())
    assert col2_metrics == {"frequent_items", "condition_count"}
    col2_conditions = set(results.get_column("column_2").get_metric("condition_count").conditions.keys())
    assert col2_conditions == {"alpha", "digit"}


@pytest.mark.parametrize("spec,msg", [
    (ResolverSpec(), "DeclarativeSchema: resolver specification must supply name or type"),
    (ResolverSpec(column_type = str), "DeclarativeSchema: resolver specification column type must be a DataType"),
    (ResolverSpec(column_name = "bruce", metrics = [MetricSpec(int)]), "DeclarativeSchema: must supply a Metric subclass to MetricSpec"),
])
def test_invalid_config(spec, msg) -> None:
    with pytest.raises(ValueError, match=msg):
        DeclarativeResolver([spec])
