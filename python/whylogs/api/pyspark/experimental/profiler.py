from functools import reduce
from logging import getLogger
from typing import Dict, Iterable, Tuple

import whylogs as why
from whylogs.core.stubs import pd
from whylogs.core.view.column_profile_view import ColumnProfileView

logger = getLogger(__name__)

try:  # type: ignore
    from pyspark.sql import DataFrame as SparkDataFrame  # types: ignore
except ImportError:  # noqa
    logger.error("No pyspark available")
    SparkDataFrame = None  # type: ignore

COL_NAME_FIELD = "col_name"

COL_PROFILE_FIELD = "col_profile"


def whylogs_pandas_map_profiler(pdf_iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for input_df in pdf_iterator:
        res = why.log(input_df)
        res_df = pd.DataFrame(columns=[COL_NAME_FIELD, COL_PROFILE_FIELD])
        for col_name, col_profile in res.view().get_columns().items():
            d = {
                COL_NAME_FIELD: [col_name],
                COL_PROFILE_FIELD: [col_profile.to_protobuf().SerializeToString()],
            }
            df_temp = pd.DataFrame(data=d)
            res_df = res_df.append(df_temp)
        yield res_df


def column_profile_bytes_aggregator(group_by_cols: Tuple[str], profiles_df: pd.DataFrame) -> pd.DataFrame:
    if len(profiles_df) == 0:
        return pd.DataFrame([group_by_cols + (bytearray(),)])

    merged_profile: ColumnProfileView = reduce(
        lambda acc, x: acc.merge(x), profiles_df[COL_PROFILE_FIELD].apply(ColumnProfileView.from_bytes)
    )
    return pd.DataFrame([group_by_cols + (merged_profile.serialize(),)])


def collect_column_profile_views(input_df: SparkDataFrame) -> Dict[str, ColumnProfileView]:
    if SparkDataFrame is None:
        logger.warning("Unable to load pyspark; install pyspark to get whylogs profiling support in spark environment.")

    cp = f"{COL_NAME_FIELD} string, {COL_PROFILE_FIELD} binary"
    profile_bytes_df = input_df.mapInPandas(whylogs_pandas_map_profiler, schema=cp)  # type: ignore
    column_profiles = profile_bytes_df.groupby(COL_NAME_FIELD).applyInPandas(  # linebreak
        column_profile_bytes_aggregator, schema=cp
    )
    collected_profile_views = {
        row.col_name: ColumnProfileView.from_bytes(row.col_profile) for row in column_profiles.collect()
    }
    return collected_profile_views
