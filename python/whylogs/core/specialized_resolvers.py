from dataclasses import dataclass
from typing import Dict, Optional

from whylogs.core.metrics.condition_count_metric import (
    Condition,
    ConditionCountConfig,
    ConditionCountMetric,
)
from whylogs.core.resolvers import MetricSpec


@dataclass(init=False)
class ConditionCountMetricSpec(MetricSpec):
    def __init__(self, conditions: Dict[str, Condition], config: Optional[ConditionCountConfig] = None):
        """
        Specify a ConditionCountMetric to instantiate.
        """
        self.metric = ConditionCountMetric
        self.config = config if config else ConditionCountConfig()
        if not isinstance(self.config, ConditionCountConfig):
            raise ValueError("ConditionCountMetricSpec: must supply a config of type ConditionCountConfig")

        self.config.conditions.update(conditions)
