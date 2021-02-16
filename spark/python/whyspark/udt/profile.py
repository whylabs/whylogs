from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame


class ModelProfileSession:
    def __init__(self, prediction_field: str, target_field: str, score_field: str):
        self.prediction_field = prediction_field
        self.target_field = target_field
        self.score_field = score_field


class WhyProfileSession:
    """
    A class that enable easy access to the profiling API
    """

    def __init__(self, dataframe: DataFrame, name: str, time_column: Optional[str] = None, group_by_columns=None,
                 model_profile: ModelProfileSession = None):
        if group_by_columns is None:
            group_by_columns = []
        self._group_by_columns = group_by_columns
        self._df = dataframe

        self._name = name
        self._time_colunn = time_column
        self._model_profile = model_profile

    def withTimeColumn(self, time_column: str):  # noqa
        """
        Set the column for grouping by time. This column must be of Timestamp type in Spark SQL.
        Note that WhyLogs uses this column to group data together, so please make sure you truncate the
        data to the appropriate level of precision (i.e. daily, hourly) before calling this.
        The API only accepts a column name (string) at the moment.

        :rtype: WhyLogSession
        """
        return WhyProfileSession(dataframe=self._df, name=self._name, time_column=time_column,
                                 group_by_columns=self._group_by_columns)

    def withModelProfile(self, prediction_field: str, target_field: str, score_field: str):  # noqa
        """
        :rtype: WhyLogSession
        """
        model_profile = ModelProfileSession(prediction_field, target_field, score_field)
        return WhyProfileSession(dataframe=self._df, name=self._name, time_column=self._time_colunn,
                                 group_by_columns=self._group_by_columns, model_profile=model_profile)

    def groupBy(self, col: str, *cols):  # noqa
        return WhyProfileSession(dataframe=self._df, name=self._name, time_column=self._time_colunn,
                                 group_by_columns=[col] + list(cols))

    def aggProfiles(self, datetime_ts: Optional[datetime] = None, timestamp_ms: int = None) -> DataFrame:  # noqa
        jvm = self._df.sql_ctx._sc._jvm  # noqa
        j_session = jvm.com.whylogs.spark.WhyLogs.newProfilingSession(self._df._jdf, self._name)  # noqa

        if self._time_colunn is not None:
            j_session = j_session.withTimeColumn(self._time_colunn)

        if len(self._group_by_columns) > 0:
            j_session = j_session.groupBy(list(self._group_by_columns))

        if self._model_profile is not None:
            metrics = self._model_profile
            j_session = j_session.withModelProfile(metrics.prediction_field,
                                                            metrics.target_field,
                                                            metrics.score_field)
        if datetime_ts is not None:
            timestamp_ms = int(datetime_ts.timestamp() * 1000)
        elif timestamp_ms is None:
            timestamp_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        jdf = j_session.aggProfiles(timestamp_ms)

        return DataFrame(jdf=jdf, sql_ctx=self._df.sql_ctx)

    def aggParquet(self, path: str, datetime_ts: Optional[datetime] = None, timestamp_ms: int = None):  # noqa
        """
        A helper method to aggregate data and write to a parquet path
        :param path: the Parquet path. In a file system that Spark supports
        :param datetime_ts: Optional. The session timestamp as a datetime object
        :param timestamp_ms: Optional. The session timestamp in milliseconds
        """
        df = self.aggProfiles(datetime_ts=datetime_ts, timestamp_ms=timestamp_ms)
        df.write.parquet(path)


def new_profiling_session(df: DataFrame, name: str, time_column: Optional[str] = None):
    if time_column is None:
        return WhyProfileSession(dataframe=df, name=name)
    else:
        return WhyProfileSession(dataframe=df, name=name, time_column=time_column)
