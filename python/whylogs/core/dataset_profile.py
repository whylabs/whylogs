import logging
import os.path
import time
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Union

from whylogs.api.writer.writer import Writable
from whylogs.core.metrics import Metric
from whylogs.core.model_performance_metrics.model_performance_metrics import (
    ModelPerformanceMetrics,
)
from whylogs.core.utils.utils import deprecated_alias

from .column_profile import ColumnProfile
from .input_resolver import _pandas_or_dict
from .schema import DatasetSchema
from .stubs import pd
from .view import DatasetProfileView

logger = logging.getLogger(__name__)

_LARGE_CACHE_SIZE_LIMIT = 1024 * 100
_MODEL_PERFORMANCE_KEY = "model_performance_metrics"


class DatasetProfile(Writable):
    """
    Dataset profile represents a collection of in-memory profiling stats for a dataset.

    Args:
        schema: :class:`DatasetSchema`, optional
            An object that represents the data column names and types
        dataset_timestamp: int, optional
            A timestamp integer that best represents the date tied to the dataset generation.
            i.e.: A January 1st 2019 Sales Dataset will have 1546300800000 as the timestamp in miliseconds (UTC).
            If None is provided, it will take the current timestamp as default
        creation_timestamp: int, optional
            The timestamp tied to the exact moment when the :class:`DatasetProfile` is created.
            If None is provided, it will take the current timestamp as default
    """

    def __init__(
        self,
        schema: Optional[DatasetSchema] = None,
        dataset_timestamp: Optional[datetime] = None,
        creation_timestamp: Optional[datetime] = None,
        metrics: Optional[Dict[str, Union[Metric, Any]]] = None,
    ):
        if schema is None:
            schema = DatasetSchema()
        now = datetime.utcnow()
        self._dataset_timestamp = dataset_timestamp or now
        self._creation_timestamp = creation_timestamp or now
        self._schema = schema
        self._columns: Dict[str, ColumnProfile] = dict()
        self._is_active = False
        self._track_count = 0
        new_cols = schema.get_col_names()
        self._initialize_new_columns(new_cols)
        self._metrics: Dict[str, Union[Metric, Any]] = metrics or dict()

    @property
    def creation_timestamp(self) -> datetime:
        return self._creation_timestamp

    @property
    def dataset_timestamp(self) -> datetime:
        return self._dataset_timestamp

    @property
    def is_active(self) -> bool:
        """Returns True if the profile tracking code is currently running."""
        return self._is_active

    @property
    def is_empty(self) -> bool:
        """Returns True if the profile tracking code is currently running."""
        return self._track_count == 0

    def set_dataset_timestamp(self, dataset_timestamp: datetime) -> None:
        if dataset_timestamp.tzinfo is None:
            logger.warning("No timezone set in the datetime_timestamp object. Default to local timezone")

        self._dataset_timestamp = dataset_timestamp.astimezone(tz=timezone.utc)

    def add_metric(self, col_name: str, metric: Metric) -> None:
        if col_name not in self._columns:
            raise ValueError(f"{col_name} is not a column in the dataset profile")
        self._columns[col_name].add_metric(metric)

    def add_dataset_metric(self, name: str, metric: Metric) -> None:
        self._metrics[name] = metric

    def add_model_performance_metrics(self, metric: ModelPerformanceMetrics) -> None:
        self._metrics[_MODEL_PERFORMANCE_KEY] = metric

    @property
    def model_performance_metrics(self) -> ModelPerformanceMetrics:
        if self._metrics:
            return self._metrics.get(_MODEL_PERFORMANCE_KEY)
        return None

    def track(
        self,
        obj: Any = None,
        *,
        pandas: Optional[pd.DataFrame] = None,
        row: Optional[Mapping[str, Any]] = None,
    ) -> None:
        try:
            self._is_active = True
            self._track_count += 1
            self._do_track(obj, pandas=pandas, row=row)
        finally:
            self._is_active = False

    def _do_track(
        self,
        obj: Any = None,
        *,
        pandas: Optional[pd.DataFrame] = None,
        row: Optional[Mapping[str, Any]] = None,
    ) -> None:

        pandas, row = _pandas_or_dict(obj, pandas, row)

        # TODO: do this less frequently when operating at row level
        dirty = self._schema.resolve(pandas=pandas, row=row)
        if dirty:
            schema_col_keys = self._schema.get_col_names()
            new_cols = (col for col in schema_col_keys if col not in self._columns)
            self._initialize_new_columns(tuple(new_cols))

        if pandas is not None:
            # TODO: iterating over each column in order assumes single column metrics
            #   but if we instead iterate over a new artifact contained in dataset profile: "MetricProfiles", then
            #   each metric profile can specify which columns its tracks, and we can call like this:
            #   metric_profile.track(pandas)
            for k in pandas.keys():
                self._columns[k].track_column(pandas[k])
            return

        if row is not None:
            for k in row.keys():
                self._columns[k].track_column([row[k]])
            return

        raise NotImplementedError

    def _track_classification_metrics(
        self,
        targets,
        predictions,
        scores=None,
        target_field=None,
        prediction_field=None,
        score_field=None,
    ) -> None:
        """
        Function to track metrics based on validation data.
        user may also pass the associated attribute names associated with
        target, prediction, and/or score.
        Parameters
        ----------
        targets : List[Union[str, bool, float, int]]
            actual validated values
        predictions : List[Union[str, bool, float, int]]
            inferred/predicted values
        scores : List[float], optional
            assocaited scores for each inferred, all values set to 1 if not
            passed
        target_field : str, optional
            Description
        prediction_field : str, optional
            Description
        score_field : str, optional
            Description
        target_field : str, optional
        prediction_field : str, optional
        score_field : str, optional
        """
        if not self.model_performance_metrics:
            self.add_model_performance_metrics(ModelPerformanceMetrics())
        self.model_performance_metrics.compute_confusion_matrix(
            predictions=predictions,
            targets=targets,
            scores=scores,
            target_field=target_field,
            prediction_field=prediction_field,
            score_field=score_field,
        )

    def _track_regression_metrics(
        self,
        targets,
        predictions,
        scores=None,
        target_field=None,
        prediction_field=None,
        score_field=None,
    ) -> None:
        """
        Function to track regression metrics based on validation data.
        user may also pass the associated attribute names associated with
        target, prediction, and/or score.
        Parameters
        ----------
        targets : List[Union[str, bool, float, int]]
            actual validated values
        predictions : List[Union[str, bool, float, int]]
            inferred/predicted values
        scores : List[float], optional
            assocaited scores for each inferred, all values set to 1 if not
            passed
        target_field : str, optional
            Description
        prediction_field : str, optional
            Description
        score_field : str, optional
            Description
        target_field : str, optional
        prediction_field : str, optional
        score_field : str, optional
        """
        if not self.model_performance_metrics:
            self.add_model_performance_metrics(ModelPerformanceMetrics())
        self.model_performance_metrics.compute_regression_metrics(
            predictions=predictions,
            targets=targets,
            scores=scores,
            target_field=target_field,
            prediction_field=prediction_field,
            score_field=score_field,
        )

    def _initialize_new_columns(self, new_cols: tuple) -> None:
        for col in new_cols:
            col_schema = self._schema.get(col)
            if col_schema:
                self._columns[col] = ColumnProfile(name=col, schema=col_schema, cache_size=self._schema.cache_size)
            else:
                logger.warning("Encountered a column without schema: %s", col)

    def view(self) -> DatasetProfileView:
        columns = {}
        for c_name, c in self._columns.items():
            columns[c_name] = c.view()
        return DatasetProfileView(
            columns=columns,
            dataset_timestamp=self.dataset_timestamp,
            creation_timestamp=self.creation_timestamp,
            metrics=self._metrics,
        )

    def flush(self) -> None:
        for col in self._columns.values():
            col.flush()

    @staticmethod
    def get_default_path(path) -> str:
        if not path.endswith("bin"):
            path = os.path.join(path, f"profile.{int(round(time.time() * 1000))}.bin")
        return path

    @deprecated_alias(path_or_base_dir="path")
    def write(self, path: Optional[str] = None, **kwargs: Any) -> None:
        output_path = self.get_default_path(path=path)
        self.view().write(output_path)
        logger.debug("Wrote profile to path: %s", output_path)

    @classmethod
    def read(cls, input_path: str) -> DatasetProfileView:
        return DatasetProfileView.read(input_path)

    def __repr__(self) -> str:
        return f"DatasetProfile({len(self._columns)} columns). Schema: {str(self._schema)}"
