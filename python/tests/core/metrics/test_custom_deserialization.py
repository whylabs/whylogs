from dataclasses import dataclass
from typing import Any, Dict

from whylogs.core.configs import SummaryConfig
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.datatypes import DataType
from whylogs.core.metrics.metric_components import MaxIntegralComponent
from whylogs.core.metrics.metrics import Metric, OperationResult
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema


@dataclass(frozen=True)
class TestMetric(Metric):
    max: MaxIntegralComponent

    @property
    def namespace(self) -> str:
        return "testmetric"

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Any]:
        return {"max": self.max.value}

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        if data.len <= 0:
            return OperationResult.ok(0)

        successes = 0

        max_ = self.max.value
        if data.numpy.ints is not None:
            max_ = max([max_, data.numpy.ints.max()])
            successes += len(data.numpy.ints)

        if data.list.ints is not None:
            l_max = max(data.list.ints)
            max_ = max([max_, l_max])
            successes += len(data.list.ints)

        self.max.set(max_)
        return OperationResult.ok(successes)

    @classmethod
    def zero(cls, schema: ColumnSchema) -> "TestMetric":
        return cls(max=MaxIntegralComponent(0))


class TestResolver(Resolver):
    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        return {"testmetric": TestMetric(max=MaxIntegralComponent(0))}


def test_custom_metric_in_profile() -> None:
    row = {"col1": 12}
    schema = DatasetSchema(
        types={
            "col1": int,
        },
        resolvers=TestResolver(),
    )
    prof = DatasetProfile(schema)
    prof.track(row=row)
    prof1_view = prof.view()
    prof1_view.write("/tmp/testmetric_in_profile")
    prof2_view = DatasetProfile.read("/tmp/testmetric_in_profile")
    prof1_cols = prof1_view.get_columns()
    prof2_cols = prof2_view.get_columns()

    assert prof1_view.get_column("col1").get_metric_component_paths() == ["testmetric/max"]
    assert prof1_cols.keys() == prof2_cols.keys()
    for col_name in prof1_cols.keys():
        col1_prof = prof1_cols[col_name]
        col2_prof = prof2_cols[col_name]
        assert (col1_prof is not None) == (col2_prof is not None)
        if col1_prof:
            assert col1_prof._metrics.keys() == col2_prof._metrics.keys()
            assert col1_prof.to_summary_dict() == col2_prof.to_summary_dict()
