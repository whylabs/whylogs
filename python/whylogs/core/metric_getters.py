from typing import Union

from whylogs.core.configs import SummaryConfig
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.metrics.metrics import Metric
from whylogs.core.relations import ValueGetter, escape
from whylogs.core.view.dataset_profile_view import DatasetProfileView


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

    def serialize(self) -> str:
        return f"::{self._metric.namespace}/{self._path}"


class ProfileGetter(ValueGetter):
    def __init__(self, profile: Union[DatasetProfile, DatasetProfileView], column_name: str, path: str) -> None:
        self._profile = profile
        self._column_name = column_name
        self._path = path

    def __call__(self) -> Union[str, int, float]:
        view = self._profile if isinstance(self._profile, DatasetProfileView) else self._profile.view()
        col_prof = view.get_column(self._column_name)
        if not col_prof:
            raise ValueError(f"Column {self._column_name} not found in profile")
        summary = col_prof.to_summary_dict()
        if self._path not in summary:
            raise ValueError(f"{self._path} is not available in {self._column_name} profile")
        return summary[self._path]

    def serialize(self) -> str:
        return f":{escape(self._column_name, ':')}:{self._path}"
