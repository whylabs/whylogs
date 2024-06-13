import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from whylogs.api.writer.writer import _Writable
from whylogs.core.metrics import Metric
from whylogs.core.model_performance_metrics.model_performance_metrics import (
    ModelPerformanceMetrics,
)
from whylogs.core.preprocessing import ColumnProperties
from whylogs.core.utils.utils import deprecated, deprecated_alias, ensure_timezone

from .column_profile import ColumnProfile
from .input_resolver import _pandas_or_dict
from .schema import DatasetSchema
from .stubs import pd
from .view import DatasetProfileView

logger = logging.getLogger(__name__)

_LARGE_CACHE_SIZE_LIMIT = 1024 * 100
_MODEL_PERFORMANCE_KEY = "model_performance_metrics"


class DatasetProfile(_Writable):
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
        metadata: Optional[Dict[str, str]] = None,
    ):
        if schema is None:
            schema = DatasetSchema()
        now = datetime.now(timezone.utc)
        self._dataset_timestamp = dataset_timestamp or now
        self._creation_timestamp = creation_timestamp or now
        self._schema = schema
        self._columns: Dict[str, ColumnProfile] = dict()
        self._is_active = False
        self._track_count = 0
        new_cols = schema.get_col_names()
        self._initialize_new_columns(new_cols)
        self._metrics: Dict[str, Union[Metric, Any]] = metrics or dict()
        self._metadata: Dict[str, str] = metadata or dict()

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

    @property
    def metadata(self) -> Dict[str, str]:
        return self._metadata

    def set_dataset_timestamp(self, dataset_timestamp: datetime) -> None:
        ensure_timezone(dataset_timestamp)
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
        execute_udfs: bool = True,
    ) -> None:
        try:
            self._is_active = True
            self._track_count += 1
            self._do_track(obj, pandas=pandas, row=row, execute_udfs=execute_udfs)
        finally:
            self._is_active = False

    def _do_track(
        self,
        obj: Any = None,
        *,
        pandas: Optional[pd.DataFrame] = None,
        row: Optional[Mapping[str, Any]] = None,
        execute_udfs: bool = True,
    ) -> None:
        pandas, row = _pandas_or_dict(obj, pandas, row)
        if execute_udfs:
            pandas, row = self._schema._run_udfs(pandas, row)

        col_id: Optional[str] = getattr(self._schema.default_configs, "identity_column", None)

        # TODO: do this less frequently when operating at row level
        dirty = self._schema.resolve(pandas=pandas, row=row)
        if dirty:
            schema_col_keys = self._schema.get_col_names()
            new_cols = (col for col in schema_col_keys if col not in self._columns)
            self._initialize_new_columns(tuple(new_cols))

        if row is not None:
            row_id = row.get(col_id) if col_id else None
            for k in row.keys():
                self._columns[k]._track_datum(row[k], row_id)
            return

        elif pandas is not None:
            # TODO: iterating over each column in order assumes single column metrics
            #   but if we instead iterate over a new artifact contained in dataset profile: "MetricProfiles", then
            #   each metric profile can specify which columns its tracks, and we can call like this:
            #   metric_profile.track(pandas)
            if pandas.empty:
                logger.warning("whylogs was passed an empty pandas DataFrame so nothing to profile in this call.")
                return
            for k in pandas.keys():
                column_values = pandas.get(k)
                if column_values is None:
                    logger.error(
                        f"whylogs was passed a pandas DataFrame with key [{k}] but DataFrame.get({k}) returned nothing!"
                    )
                    return

                dtype = self._schema.types.get(k)
                homogeneous = (
                    dtype is not None
                    and isinstance(dtype, tuple)
                    and len(dtype) == 2
                    and isinstance(dtype[1], ColumnProperties)
                    and dtype[1] == ColumnProperties.homogeneous
                )

                id_values = pandas.get(col_id) if col_id else None
                if col_id is not None and id_values is None:
                    logger.warning(f"identity column was passed as {col_id} but column was not found in the dataframe.")

                if homogeneous:
                    self._columns[k]._track_homogeneous_column(column_values, id_values)
                else:
                    self._columns[k].track_column(column_values, id_values)

            return

        raise NotImplementedError

    def _track_classification_metrics(self, targets, predictions, scores=None) -> None:
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
        self.model_performance_metrics.compute_confusion_matrix(predictions=predictions, targets=targets, scores=scores)

    def _track_regression_metrics(self, targets, predictions, scores=None) -> None:
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
            predictions=predictions, targets=targets, scores=scores
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
            metadata=self._metadata,
        )

    def flush(self) -> None:
        for col in self._columns.values():
            col.flush()

    def _get_default_filename(self) -> str:
        return f"profile.{int(round(time.time() * 1000))}.bin"

    @deprecated(message="please use a Writer")
    def write(self, path: Optional[str] = None, **kwargs: Any) -> Tuple[bool, str]:
        success, files = self._write("", path, **kwargs)
        files = files[0] if isinstance(files, list) else files
        return success, files

    @deprecated_alias(path_or_base_dir="path")
    def _write(
        self, path: Optional[str] = None, filename: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[str]]]:
        filename = filename or self._get_default_filename()
        success, files = self.view()._write(path, filename, **kwargs)
        logger.debug(f"Wrote profile to path: {files}")
        return success, files

    @classmethod
    def read(cls, input_path: str) -> DatasetProfileView:
        return DatasetProfileView.read(input_path)

    def __repr__(self) -> str:
        return f"DatasetProfile({len(self._columns)} columns). Schema: {str(self._schema)}"
