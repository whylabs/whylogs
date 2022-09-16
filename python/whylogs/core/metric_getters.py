from typing import Union

from whylogs.core.relations import ValueGetter
from whylogs.core.configs import SummaryConfig
from whylogs.core.dataset_profile import DatasetProfile, DatasetProfileView
from whylogs.core.metrics.metrics import Metric


class MetricGetter(ValueGetter):
    def __init__(self, metric: Metric, path: str) -> None:
        self._metric = metric
        self._path = path
        summary = self._metric.to_summary_dict(SummaryConfig())
        if path not in summary:
            raise ValueError(f"{path} is not available in {metric.namespace}")

    def __call__(self) -> Union[str, int, float]:
        summary = self._metric.to_summary_dict(SummaryConfig())
        return summary[self._path]


class ProfileGetter(ValueGetter):
    def __init__(self, profile: DatasetProfile, column_name: str, path: str) -> None:
        self._profile = profile
        self._column_name = column_name
        self._path = path

    def __call__(self) -> Union[str, int, float]:
        col_prof = self._profile.view().get_column(self._column_name)
        if not col_prof:
            raise ValueError(f"Column {self._column_name} not found in profile")
        summary = col_prof.to_summary_dict()
        print(summary)
        if self._path not in summary:
            raise ValueError(f"{self._path} is not available in {self._column_name} profile")
        return summary[self._path]
