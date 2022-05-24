import logging
import os.path
import time
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

from .column_profile import ColumnProfile
from .schema import DatasetSchema
from .stubs import pd
from .view import DatasetProfileView

logger = logging.getLogger(__name__)

_LARGE_CACHE_SIZE_LIMIT = 1024 * 100


class DatasetProfile(object):
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
        if obj is not None:
            if pandas is not None:
                raise ValueError("Cannot pass both obj and pandas params")
            if row is not None:
                raise ValueError("Cannot pass both obj and row params")

            if isinstance(obj, pd.DataFrame):
                pandas = obj
            elif isinstance(obj, (dict, Dict, Mapping)):
                row = obj

        if pandas is not None and row is not None:
            raise ValueError("Cannot pass both pandas and row params")

        # TODO: do this less frequently when operating at row level
        dirty = self._schema.resolve(pandas=pandas, row=row)
        if dirty:
            new_cols = self._schema.get_col_names().difference(self._columns.keys())
            for col in new_cols:
                col_schema = self._schema.get(col)
                if col_schema:
                    self._columns[col] = ColumnProfile(name=col, schema=col_schema, cache_size=self._schema.cache_size)
                else:
                    logger.warning("Encountered a column without schema: %s", col)

        if pandas is not None:
            for k in pandas.keys():
                self._columns[k].track_column(pandas[k])
            return

        if row is not None:
            for k in row.keys():
                self._columns[k].track_column([row[k]])
            return

        raise NotImplementedError

    def view(self) -> DatasetProfileView:
        columns = {}
        for c_name, c in self._columns.items():
            columns[c_name] = c.view()
        return DatasetProfileView(
            columns=columns, dataset_timestamp=self.dataset_timestamp, creation_timestamp=self.creation_timestamp
        )

    def flush(self) -> None:
        for col in self._columns.values():
            col.flush()

    def write(self, path_or_base_dir: str) -> None:
        if not path_or_base_dir.endswith(".bin"):
            output_path = os.path.join(path_or_base_dir, f"profile.{int(round(time.time() * 1000))}.bin")
        else:
            output_path = path_or_base_dir

        self.view().write(output_path)
        logger.debug("Wrote profile to path: %s", output_path)

    @classmethod
    def read(cls, input_path: str) -> DatasetProfileView:
        return DatasetProfileView.read(input_path)

    def __repr__(self) -> str:
        return f"DatasetProfile({len(self._columns)} columns). Schema: {str(self._schema)}"
