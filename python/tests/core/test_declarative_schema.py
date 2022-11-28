from whylogs.core.declarative_schema import DeclarativeSchema, MetricSpc, ResolverSpec
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.condition_count_metric import ConditionCountConfig, ConditionCountMetric
from whylogs.core.metrics.condition_count_metric import Relation as Rel
from whylogs.core.metrics.condition_count_metric import relation as rel


def test_declarative_schema() -> None:
    
    schema = DeclarativeSchema(
        [
            ResolverSpec(
                column_name = "column_1",
                metrics = [
                    MetricSpec(StandardMetric.distribution),
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
                    MetricSpec(StandardMetric.frequent_items),
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


def test_invalid_config() -> None:
    pass
