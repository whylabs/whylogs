from datetime import datetime
from functools import reduce
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from fugue import FugueWorkflow, PartitionSpec, Schema, transform, transformer

import whylogs as why
from whylogs.core.view.column_profile_view import ColumnProfileView
from whylogs.core.view.dataset_profile_view import DatasetProfileView

COL_NAME_FIELD = "__whylogs_fugue_col_name"
COL_PROFILE_FIELD = "__whylogs_fugue_col_profile"
COL_PROFILE_SCHEMA = Schema([(COL_NAME_FIELD, str), (COL_PROFILE_FIELD, bytes)])
DF_PROFILE_FIELD = "__whylogs_df_profile_view"


class _Profiler:
    def __init__(
        self,
        partition,
        cols,
        dataset_timestamp: Optional[datetime] = None,
        creation_timestamp: Optional[datetime] = None,
        profile_field: str = DF_PROFILE_FIELD,
    ):
        now = datetime.utcnow()

        self._dataset_timestamp = dataset_timestamp or now
        self._creation_timestamp = creation_timestamp or now

        self._partition = PartitionSpec(partition)
        self._by = self._partition.partition_by
        self._cols = cols
        self._profile_field = profile_field
        self._profile_schema = Schema([(profile_field, bytes)])

    def to_col_profiles(self, df: pd.DataFrame) -> Iterable[Dict[str, Any]]:
        res = why.log(df[self._cols] if self._cols is not None else df)
        for col_name, col_profile in res.view().get_columns().items():
            yield {COL_NAME_FIELD: col_name, COL_PROFILE_FIELD: col_profile.serialize()}

    def merge_col_profiles(self, df: pd.DataFrame) -> pd.DataFrame:
        merged_profile: ColumnProfileView = reduce(
            lambda acc, x: acc.merge(x), df[COL_PROFILE_FIELD].apply(ColumnProfileView.deserialize)
        )
        return df.head(1).assign(**{COL_PROFILE_FIELD: merged_profile.serialize()})

    def merge_to_view(self, col_profiles: List[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        profile_view = DatasetProfileView(
            columns={row[COL_NAME_FIELD]: ColumnProfileView.from_bytes(row[COL_PROFILE_FIELD]) for row in col_profiles},
            dataset_timestamp=self._dataset_timestamp,
            creation_timestamp=self._creation_timestamp,
        )
        yield {self._profile_field: profile_view.serialize()}

    def transform_no_logical_partition(self, df: Any, **kwargs: Any) -> DatasetProfileView:
        dag = FugueWorkflow()
        input_df = dag.load(df) if isinstance(df, str) else dag.df(df)
        cols = input_df.partition(self._partition).transform(
            self.to_col_profiles,
            schema=[(COL_NAME_FIELD, str), (COL_PROFILE_FIELD, bytes)],
        )
        merged_cols = cols.partition_by(COL_NAME_FIELD).transform(self.merge_col_profiles, schema="*")
        result = merged_cols.process(self.merge_to_view, schema=self._profile_schema)
        result.yield_dataframe_as("result", as_local=True)
        return DatasetProfileView.deserialize(dag.run(**kwargs)["result"].as_array()[0][0])

    def profile_partition(self, df: pd.DataFrame) -> pd.DataFrame:
        res = why.log(df[self._cols] if self._cols is not None else df).view().serialize()
        return df.head(1)[self._by].assign(**{self._profile_field: res})

    def transform_with_logical_partition(self, df: Any, **kwargs: Any) -> Any:
        return transform(
            df,
            transformer(lambda pdf: pdf.schema.extract(self._by) + self._profile_schema)(self.profile_partition),
            partition=self._partition,
            **kwargs,
        )


def profile(
    df: Any,
    dataset_timestamp: Optional[datetime] = None,
    creation_timestamp: Optional[datetime] = None,
    partition: Any = None,
    profile_cols: Optional[List[str]] = None,
    as_local: bool = True,
    profile_field: str = DF_PROFILE_FIELD,
    **kwargs,
) -> Any:
    profiler = _Profiler(
        partition,
        profile_cols,
        dataset_timestamp=dataset_timestamp,
        creation_timestamp=creation_timestamp,
        profile_field=profile_field,
    )
    if len(profiler._by) == 0:
        return profiler.transform_no_logical_partition(df, **kwargs)
    return profiler.transform_with_logical_partition(df, as_local=as_local, **kwargs)
