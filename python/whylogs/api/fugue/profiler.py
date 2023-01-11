from datetime import datetime, timezone
from functools import reduce
from typing import Any, Dict, Iterable, List, Optional

import fugue.api as fa
import pandas as pd
from fugue import PartitionSpec, Schema, transformer

import whylogs as why
from whylogs.core import DatasetSchema
from whylogs.core.view.column_profile_view import ColumnProfileView
from whylogs.core.view.dataset_profile_view import DatasetProfileView

_COL_NAME_FIELD = "__whylogs_fugue_col_name"
_COL_PROFILE_FIELD = "__whylogs_fugue_col_profile"
DF_PROFILE_FIELD = "__whylogs_df_profile_view"


def fugue_profile(
    df: Any,
    dataset_timestamp: Optional[datetime] = None,
    creation_timestamp: Optional[datetime] = None,
    partition: Any = None,
    profile_cols: Optional[List[str]] = None,
    schema: Optional[DatasetSchema] = None,
    as_local: bool = True,
    profile_field: str = DF_PROFILE_FIELD,
    engine: Any = None,
    engine_conf: Any = None,
    **kwargs,
) -> Any:
    profiler = _FugueProfiler(
        partition,
        profile_cols,
        dataset_timestamp=dataset_timestamp,
        creation_timestamp=creation_timestamp,
        schema=schema,
        profile_field=profile_field,
    )
    if len(profiler._by) == 0:
        assert len(kwargs) == 0, ValueError("Profiling without partition can not take extra arguments")
        return profiler.transform_no_logical_partition(df, engine=engine, engine_conf=engine_conf)
    return profiler.transform_with_logical_partition(
        df, engine=engine, engine_conf=engine_conf, as_local=as_local, **kwargs
    )


class _FugueProfiler:
    def __init__(
        self,
        partition,
        cols,
        dataset_timestamp: Optional[datetime] = None,
        creation_timestamp: Optional[datetime] = None,
        schema: Optional[DatasetSchema] = None,
        profile_field: str = DF_PROFILE_FIELD,
    ):
        now = datetime.now(timezone.utc)

        self._dataset_timestamp = dataset_timestamp or now
        self._creation_timestamp = creation_timestamp or now

        part = PartitionSpec(partition)
        self._by = part.partition_by
        self._partition = PartitionSpec(part, by=self._by)
        self._cols = cols
        self._profile_field = profile_field
        self._profile_schema = Schema([(profile_field, bytes)])
        self._schema = schema

    def to_col_profiles(self, df: pd.DataFrame) -> Iterable[Dict[str, Any]]:
        res = why.log(df[self._cols] if self._cols is not None else df, schema=self._schema)
        for col_name, col_profile in res.view().get_columns().items():
            yield {_COL_NAME_FIELD: col_name, _COL_PROFILE_FIELD: col_profile.serialize()}

    def merge_col_profiles(self, df: pd.DataFrame) -> pd.DataFrame:
        merged_profile: ColumnProfileView = reduce(
            lambda acc, x: acc.merge(x), df[_COL_PROFILE_FIELD].apply(lambda x: ColumnProfileView.deserialize(x))
        )
        return df.head(1).assign(**{_COL_PROFILE_FIELD: merged_profile.serialize()})

    def merge_to_view(self, col_profiles: Iterable[Dict[str, Any]]) -> DatasetProfileView:
        return DatasetProfileView(
            columns={
                row[_COL_NAME_FIELD]: ColumnProfileView.from_bytes(row[_COL_PROFILE_FIELD]) for row in col_profiles
            },
            dataset_timestamp=self._dataset_timestamp,
            creation_timestamp=self._creation_timestamp,
        )

    def profile_partition(self, df: pd.DataFrame) -> pd.DataFrame:
        res = why.log(df[self._cols] if self._cols is not None else df, schema=self._schema).view().serialize()
        return df.head(1)[self._by].assign(**{self._profile_field: res})  # type: ignore

    # ---------------- Starting Fugue related logic

    def transform_no_logical_partition(self, df: Any, engine: Any, engine_conf: Any) -> DatasetProfileView:
        with fa.engine_context(engine, engine_conf):
            input_df = fa.load(df, as_fugue=True) if isinstance(df, str) else fa.as_fugue_df(df)
            cols = fa.transform(
                input_df,
                self.to_col_profiles,
                schema=[(_COL_NAME_FIELD, str), (_COL_PROFILE_FIELD, bytes)],
                partition=self._partition,
            )
            merged_cols = fa.transform(
                cols, self.merge_col_profiles, schema="*", partition=_COL_NAME_FIELD, as_local=True
            )
            return self.merge_to_view(fa.as_dict_iterable(merged_cols))

    def transform_with_logical_partition(self, df: Any, engine: Any, engine_conf: Any, **kwargs: Any) -> Any:
        return fa.transform(
            df,
            transformer(lambda pdf: pdf.schema.extract(self._by) + self._profile_schema)(self.profile_partition),
            partition=self._partition,
            engine=engine,
            engine_conf=engine_conf,
            **kwargs,
        )
