import logging
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, TypeVar

from whylogs.core.datatypes import StandardTypeMapper, TypeMapper
from whylogs.core.metrics.metrics import Metric, MetricConfig
from whylogs.core.resolvers import Resolver, StandardResolver
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.stubs import pd
from whylogs.core.validators.validator import Validator

logger = logging.getLogger(__name__)

LARGE_CACHE_SIZE_LIMIT = 1024 * 100

T = TypeVar("T", bound="DatasetSchema")


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
    >>> from whylogs.core.resolvers import Resolver, StandardResolver
    >>>
    >>> class MyResolver(StandardResolver):
    ...    pass
    >>>
    >>> schema = DatasetSchema(
    ...    types={
    ...        "col1": str,
    ...        "col2": np.int32,
    ...        "col3": pd.CategoricalDtype(categories=('foo', 'bar'), ordered=True)
    ...    },
    ...    resolvers=MyResolver()
    ... )
    >>> prof = DatasetProfile(schema)
    >>> df = pd.DataFrame({"col1": ['foo'], "col2": np.array([1], dtype=np.int32), "col3": ['bar']})
    >>> prof.track(pandas=df)

    """

    def __init__(
        self,
        types: Optional[Dict[str, Any]] = None,
        default_configs: Optional[MetricConfig] = None,
        type_mapper: Optional[TypeMapper] = None,
        resolvers: Optional[Resolver] = None,
        cache_size: int = 1024,
        schema_based_automerge: bool = False,
        segments: Optional[Dict[str, SegmentationPartition]] = None,
        validators: Dict[str, List[Validator]] = None,
    ) -> None:
        self._columns = dict()
        self.types = types or dict()
        self.default_configs = default_configs or MetricConfig()
        self.type_mapper = type_mapper or StandardTypeMapper()
        self.resolvers = resolvers or StandardResolver()
        self.cache_size = cache_size
        self.schema_based_automerge = schema_based_automerge
        self.segments = segments or dict()
        self.validators = validators or dict()

        if self.cache_size < 0:
            logger.warning("Negative cache size value. Disabling caching")
            self.cache_size = 0

        if self.cache_size > LARGE_CACHE_SIZE_LIMIT:
            logger.warning(
                "Cache size value %s is larger than recommended limit of %s",
                self.cache_size,
                LARGE_CACHE_SIZE_LIMIT,
            )

        for col, data_type in self.types.items():
            self._columns[col] = ColumnSchema(
                dtype=data_type,
                resolver=self.resolvers,
                type_mapper=self.type_mapper,
                cfg=self.default_configs,
                validators=self.validators,
            )

    def copy(self) -> "DatasetSchema":
        """Returns a new instance of the same underlying schema"""
        key_dict = vars(self).copy()
        key_dict.pop("_columns")
        keys = key_dict.keys()
        args = {k: deepcopy(self.__dict__[k]) for k in keys if k not in self.types}
        copy = self.__class__(**args)
        copy._columns = deepcopy(self._columns)
        copy.segments = self.segments.copy()
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
                    cfg=self.default_configs,
                    resolver=self.resolvers,
                    validators=self.validators,
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
                cfg=self.default_configs,
                resolver=self.resolvers,
                validators=self.validators,
                type_mapper=self.type_mapper,
            )
            dirty = True

        return dirty

    def get_col_names(self) -> tuple:
        return tuple(self._columns.keys())

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
    cfg: MetricConfig = MetricConfig()
    type_mapper: TypeMapper = StandardTypeMapper()
    resolver: Resolver = StandardResolver()
    validators: Dict[str, List[Validator]] = field(default_factory=dict)

    def get_metrics(self, name: str) -> Dict[str, Metric]:
        return self.resolver.resolve(name=name, why_type=self.type_mapper(self.dtype), column_schema=self)

    def get_validators(self, name: str) -> List[Optional[Validator]]:
        if self.validators:
            return self.validators.get(name, [])
        return []
