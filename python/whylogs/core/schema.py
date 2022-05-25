import logging
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, TypeVar

from whylogs.core.datatypes import StandardTypeMapper, TypeMapper
from whylogs.core.metrics import Metric
from whylogs.core.resolvers import Resolver, StandardResolver
from whylogs.core.stubs import pd

logger = logging.getLogger(__name__)

LARGE_CACHE_SIZE_LIMIT = 1024 * 100

T = TypeVar("T", bound="DatasetSchema")


@dataclass(frozen=True)
class ColumnConfig:
    hll_lg_k: int = 12
    kll_k: int = 256
    fi_lg_max_k: int = 10  # 128 entries
    fi_disabled: bool = False
    unicode_ranges = {
        "emoji": (0x1F600, 0x1F64F),
        "control": (0x00, 0x1F),
        "digits": (0x30, 0x39),
        "latin-lower": (0x41, 0x5A),
        "latin-upper": (0x61, 0x7A),
        "basic-latin": (0x00, 0x7F),
        "extended-latin": (0x0080, 0x02AF),
    }


class DatasetSchema:
    """
    Defines the schema for tracking metrics in whylogs.

    In order to customize your tracking, you can extend this class to specify your own column schema
    or your own type resolution. Otherwise, you can just use the default DatasetSchema object.

    Schema objects are also used to group datasets together.

    Attributes:
        types: required. a dictionay of column name to the Python type.

        default_configs: optional. Options to configure various behavior of whylogs.

        type_mapper: Optional. a mapper that transates the Python type to standardized whylogs :class:`DataType` object.

        resolvers: Optional. an object that defines how to map from a column name, a whylogs :class:`DataType` and a
                   schema to metrics.


    Examples
    --------
    >>> import pandas as pd
    >>> import numpy as np
    >>> from whylogs.core import DatasetSchema, DatasetProfile
    >>>
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
    >>> prof.track(pandas=df)

    """

    types: Dict[str, Any] = {}
    default_configs: ColumnConfig = ColumnConfig()
    type_mapper: TypeMapper = StandardTypeMapper()
    resolvers: Resolver = StandardResolver()
    cache_size: int = 1024
    schema_based_automerge: bool = False

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
                    dtype=tpe, resolver=self.resolvers, type_mapper=self.type_mapper, cfg=self.default_configs
                )

    def copy(self) -> "DatasetSchema":
        """Returns a new instance of the same underlying schema"""
        copy = DatasetSchema()
        copy._columns = self._columns.copy()
        copy.cache_size = self.cache_size
        copy.type_mapper = self.type_mapper
        copy.resolvers = self.resolvers

        return copy

    def resolve(self, *, pandas: Optional[pd.DataFrame] = None, row: Optional[Mapping[str, Any]] = None) -> bool:
        if pandas is not None:
            return self._resolve_pdf(pandas)

        if row is not None:
            for k, v in row.items():
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
        """
        Resolve ColumnSchema from the dataframe. We only resolve newly detected
        columns unless `force_resolve` is set to True.
        """
        col_names = df.dtypes.keys()
        dirty = False
        for col_name in col_names:
            if not force_resolve and col_name in self._columns:
                logger.debug("column is already configured")
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

    def get(self, name: str) -> Optional["ColumnSchema"]:
        return self._columns.get(name)


@dataclass(frozen=True)
class ColumnSchema:
    """
    Schema of a column.

    The main goal is to specify the data type.
    On top of that, users can configure their own tracker resolution logic (mapping a type to a list of tracker
    factories) and any additional trackers here.
    """

    dtype: Any
    cfg: ColumnConfig = ColumnConfig()
    type_mapper: TypeMapper = StandardTypeMapper()
    resolver: Resolver = StandardResolver()

    def get_metrics(self, name: str) -> Dict[str, Metric]:
        return self.resolver.resolve(name=name, why_type=self.type_mapper(self.dtype), column_schema=self)
