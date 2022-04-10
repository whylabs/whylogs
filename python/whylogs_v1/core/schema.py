import logging
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, TypeVar, Union

from whylogs_v1.core.datatypes import StandardTypeMapper, TypeMapper
from whylogs_v1.core.resolvers import Resolver, StandardResolver
from whylogs_v1.core.stubs import pd
from whylogs_v1.core.trackers import Tracker

logger = logging.getLogger(__name__)

LARGE_CACHE_SIZE_LIMIT = 1024 * 100


@dataclass(frozen=True)
class ColumnSchema:
    """
    Schema of a column.

    The main goal is to specify the data type.
    On top of that, users can configure their own tracker resolution logic (mapping a type to a list of tracker
    factories) and any additional trackers here.
    """

    dtype: Any
    type_mapper: TypeMapper = StandardTypeMapper()
    resolver: Resolver = StandardResolver()

    def get_trackers(self, name: str) -> Dict[str, Tracker]:
        return self.resolver.resolve(name=name, why_type=self.type_mapper(self.dtype))


T = TypeVar("T", bound="DatasetSchema")


class DatasetSchema:
    """
    Defines the schema for tracking metrics in whylogs.

    In order to customize your tracking, you can extend this class to specify your own column schema
    or your own type resolution. Otherwise, you can just use the default DatasetSchema object.

    Schema objects are also used to group datasets together.

    Examples
    --------
    >>> import pandas as pd
    >>> import numpy as np
    >>> from whylogs_v1.core import DatasetSchema, DatasetProfile

    >>> class MySchema(DatasetSchema):
    ...    types = {
    ...        "col1": str,
    ...        "col2": np.int32,
    ...        "col3": pd.Categorical(categories=('foo', 'bar'), ordered=True)
    ...    }
    ...    cache_size = 12
    >>> schema = MySchema()
    >>> prof = DatasetProfile(MySchema())
    >>> df = pd.DataFrame({"col1": ['foo'], "col2": np.array([1], dtype=np.int32), "col3": ['bar']})
    >>> prof.track(df)

    """

    types: Dict[str, Any] = {}
    type_mapper: TypeMapper = StandardTypeMapper()
    resolvers: Resolver = StandardResolver()
    cache_size: int = 1024

    def __init__(self) -> None:
        self._columns = {}

        if self.cache_size < 0:
            logger.warning("Negative cache size value. Disabling caching")
            self.cache_size = 0

        if self.cache_size > LARGE_CACHE_SIZE_LIMIT:
            logger.warning(
                "Cache size value %s is larger than recommended limit of %s",
                self.cache_size,
                LARGE_CACHE_SIZE_LIMIT,
            )

        if self.types:
            for col, tpe in self.types.items():
                self._columns[col] = ColumnSchema(
                    dtype=tpe,
                    resolver=self.resolvers,
                    type_mapper=self.type_mapper,
                )

    def copy(self) -> "DatasetSchema":
        """Returns a new instance of the same underlying schema"""
        copy = DatasetSchema()
        copy._columns = self._columns.copy()
        self.cache_size = self.cache_size
        self.type_mapper = self.type_mapper
        copy.resolvers = self.resolvers

        return copy

    def resolve(self, dataset_or_row: Union[pd.DataFrame, Mapping[str, Any]]) -> bool:
        if isinstance(dataset_or_row, pd.DataFrame):
            return self._resolve_pdf(dataset_or_row)

        if isinstance(dataset_or_row, Mapping):
            for k, v in dataset_or_row.items():
                if k in self._columns:
                    continue
                self._columns[k] = ColumnSchema(
                    dtype=type(v),
                    resolver=self.resolvers,
                    type_mapper=self.type_mapper,
                )
            return True
        raise NotImplementedError

    def _resolve_pdf(self, df: pd.DataFrame, force_resolve: bool = False) -> bool:
        """Resolve ColumnSchema from the dataframe.

        We only resolve newly detected columns unless `force_resolve` is set to True."""
        col_names = df.dtypes.keys()
        dirty = False
        for col_name in col_names:
            if not force_resolve and col_name in self._columns:
                # column is already configured
                continue

            col_dtype = df.dtypes[col_name]
            self._columns[col_name] = ColumnSchema(
                dtype=col_dtype,
                resolver=self.resolvers,
                type_mapper=self.type_mapper,
            )
            dirty = True

        return dirty

    def get_col_names(self) -> frozenset:
        return frozenset(self._columns.keys())

    def get(self, name: str) -> Optional[ColumnSchema]:
        return self._columns.get(name)
