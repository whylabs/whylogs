from typing import Dict, List

import whylogs as why
from whylogs.core.constraints import ConstraintsBuilder
from whylogs.core.constraints.factories import condition_meets
from whylogs.core.datatypes import DataType
from whylogs.core.metrics import Metric, StandardMetric
from whylogs.core.metrics.condition_count_metric import (
    Condition,
    ConditionCountConfig,
    ConditionCountMetric,
)
from whylogs.core.metrics.condition_count_metric import Relation as Rel
from whylogs.core.metrics.condition_count_metric import not_relation as not_rel
from whylogs.core.metrics.condition_count_metric import relation as rel
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema


class CustomResolver(Resolver):
    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        metrics: List[StandardMetric] = [StandardMetric.counts, StandardMetric.types]
        result: Dict[str, Metric] = {}
        for m in metrics:
            result[m.name] = m.zero(column_schema.cfg)
        if name in ["legs", "animal", "weight"]:
            result["condition_count"] = ConditionCountMetric.zero(column_schema.cfg)

        return result


def test_condition_count_constrain(pandas_dataframe):
    conditions = {
        "not_4": Condition(not_rel(rel(Rel.equal, 4))),
    }
    resolver = CustomResolver()
    config = ConditionCountConfig(conditions=conditions)
    schema = DatasetSchema(default_configs=config, resolvers=resolver)

    prof_view = why.log(pandas_dataframe, schema=schema).profile().view()
    prof_view.to_pandas()

    builder = ConstraintsBuilder(dataset_profile_view=prof_view)
    builder.add_constraint(condition_meets(column_name="legs", condition_name="not_4"))
    builder.add_constraint(condition_meets(column_name="animal", condition_name="not_4"))
    builder.add_constraint(condition_meets(column_name="weight", condition_name="not_4"))

    constraints = builder.build()
    rp = constraints.generate_constraints_report()
    assert rp[0].name == "legs meets condition not_4" and rp[0].failed == 1
    assert rp[1].name == "animal meets condition not_4" and rp[0].failed == 0
    assert rp[2].name == "weight meets condition not_4" and rp[0].failed == 0
