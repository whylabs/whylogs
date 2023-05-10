import pandas as pd
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
from whylogs.core.specialized_resolvers import ConditionCountMetricSpec
from whylogs.experimental.core.udf_schema import UdfSchema, UdfSpec


def test_udf_row() -> None:
    schema = UdfSchema(
        STANDARD_RESOLVER,
        udf_specs=[UdfSpec(column_name="col1", udfs={"col2": lambda x: x, "col3": lambda x: x})],
    )
    data = {"col1": 42}
    results = why.log(row=data, schema=schema).view()
    col1 = results.get_column("col1").to_summary_dict()
    col2 = results.get_column("col1.col2").to_summary_dict()
    col3 = results.get_column("col1.col3").to_summary_dict()
    assert col1 == col2 == col3


def test_udf_pandas() -> None:
    schema = UdfSchema(
        STANDARD_RESOLVER,
        udf_specs=[UdfSpec(column_name="col1", udfs={"col2": lambda x: x, "col3": lambda x: x})],
    )
    data = pd.DataFrame({"col1": [42, 12, 7]})
    results = why.log(pandas=data, schema=schema).view()
    col1 = results.get_column("col1").to_summary_dict()
    col2 = results.get_column("col1.col2").to_summary_dict()
    col3 = results.get_column("col1.col3").to_summary_dict()
    assert col1 == col2 == col3
