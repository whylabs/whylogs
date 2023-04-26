import logging
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from whylogs.core.datatypes import DataType, TypeMapper
from whylogs.core.metrics.metrics import MetricConfig
from whylogs.core.resolvers import ResolverSpec
from whylogs.core.schema import DatasetSchema, DeclarativeSchema
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.stubs import pd
from whylogs.core.validators.validator import Validator

logger = logging.getLogger(__name__)


@dataclass
class UdfSpec:
    column_name: Optional[str] = None  # TODO: maybe make this a regex
    column_type: Optional[DataType] = None
    udfs: Dict[str, Callable[[Any], Any]] = field(
        default_factory=dict
    )  # new column name -> callable to compute new column value


class UdfSchema(DeclarativeSchema):
    def __init__(
        self,
        resolvers: List[ResolverSpec],
        types: Optional[Dict[str, Any]] = None,
        default_config: Optional[MetricConfig] = None,
        type_mapper: Optional[TypeMapper] = None,
        cache_size: int = 1024,
        schema_based_automerge: bool = False,
        segments: Optional[Dict[str, SegmentationPartition]] = None,
        validators: Optional[Dict[str, List[Validator]]] = None,
        udf_specs: Optional[List[UdfSpec]] = None,
    ) -> None:
        super().__init__(
            resolvers=resolvers,
            types=types,
            default_config=default_config,
            type_mapper=type_mapper,
            cache_size=cache_size,
            schema_based_automerge=schema_based_automerge,
            segments=segments,
            validators=validators,
        )
        self.udf_specs = udf_specs or []

    def copy(self) -> DatasetSchema:
        copy = super().copy()
        copy.udf_specs = deepcopy(self.udf_specs)
        return copy

    def _run_udfs_on_row(self, row: Mapping[str, Any], new_columns: Mapping[str, Any]) -> None:
        for column, value in row.items():
            for udf_spec in self.udf_specs:
                col_name, col_type = udf_spec.column_name, udf_spec.column_type
                why_type = self.type_mapper(type(value))
                if col_name == column or (col_name is None and isinstance(col_type, why_type)):
                    for new_col, udf in udf_spec.udfs.items():
                        if new_col in new_columns:
                            logger.info(f"UDF {udf.__name__} overwriting column {new_col}")
                        new_columns[new_col] = udf(value)  # type: ignore

    def _run_udfs_on_dataframe(self, pandas: pd.DataFrame, new_df: pd.DataFrame) -> None:
        for column in pandas.keys():
            for udf_spec in self.udf_specs:
                col_name, col_type = udf_spec.column_name, udf_spec.column_type
                why_type = pandas.dtypes[column]
                if col_name == column or (col_name is None and isinstance(col_type, why_type)):
                    for new_col, udf in udf_spec.udfs.items():
                        if new_col in new_df.keys():
                            logger.info(f"UDF {udf.__name__} overwriting column {new_col}")
                        new_df[new_col] = pandas[column].map(udf)

    def _run_udfs(
        self, pandas: Optional[pd.DataFrame] = None, row: Optional[Mapping[str, Any]] = None
    ) -> Tuple[Optional[pd.DataFrame], Optional[Mapping[str, Any]]]:
        new_columns = deepcopy(row) if row else None
        new_df = pd.DataFrame(pandas) if pandas is not None else None
        if row is not None:
            self._run_udfs_on_row(row, new_columns)  # type: ignore

        if pandas is not None:
            self._run_udfs_on_dataframe(pandas, new_df)

        return new_df, new_columns
