import os
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

    def withModelProfile(self, prediction_field: str, target_field: str, score_field: str = None):  # noqa
        """
        Track model performance. Specify the prediction field, target field and score field.

        If score_field is not specified, the profiler will track regression metrics.
        If score_field is specified, the profiler will track classification metrics.
        :rtype: WhyLogSession
        """
        model_profile = ModelProfileSession(prediction_field, target_field, score_field)
        return WhyProfileSession(dataframe=self._df, name=self._name, time_column=self._time_colunn,
                                 group_by_columns=self._group_by_columns, model_profile=model_profile)

    def groupBy(self, col: str, *cols):  # noqa
        return WhyProfileSession(dataframe=self._df, name=self._name, time_column=self._time_colunn,
                                 group_by_columns=[col] + list(cols))

    def aggProfiles(self, datetime_ts: Optional[datetime] = None, timestamp_ms: int = None) -> DataFrame:  # noqa
        if datetime_ts is not None:
            timestamp_ms = int(datetime_ts.timestamp() * 1000)
        elif timestamp_ms is None:
            timestamp_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        jdf = self._create_j_session().aggProfiles(timestamp_ms)

        return DataFrame(jdf=jdf, sql_ctx=self._df.sql_ctx)

    def _create_j_session(self):
        jvm = self._df.sql_ctx._sc._jvm  # noqa
        j_session = jvm.com.whylogs.spark.WhyLogs.newProfilingSession(self._df._jdf, self._name)  # noqa
        if self._time_colunn is not None:
            j_session = j_session.withTimeColumn(self._time_colunn)
        if len(self._group_by_columns) > 0:
            j_session = j_session.groupBy(list(self._group_by_columns))
        if self._model_profile is not None:
            mp = self._model_profile
            if mp.score_field:
                j_session = j_session.withModelProfile(mp.prediction_field,
                                                       mp.target_field,
                                                       mp.score_field)
            else:
                j_session = j_session.withModelProfile(mp.prediction_field,
                                                       mp.target_field)
        return j_session

    def aggParquet(self, path: str, datetime_ts: Optional[datetime] = None, timestamp_ms: int = None):  # noqa
        """
        A helper method to aggregate data and write to a parquet path
        :param path: the Parquet path. In a file system that Spark supports
        :param datetime_ts: Optional. The session timestamp as a datetime object
        :param timestamp_ms: Optional. The session timestamp in milliseconds
        """
        df = self.aggProfiles(datetime_ts=datetime_ts, timestamp_ms=timestamp_ms)
        df.write.parquet(path)

    def log(self, dt: Optional[datetime] = None, org_id: str = None, model_id: str = None, api_key: str = None,
            endpoint: str = "https://api.whylabsapp.com"):
        """
        Run profiling and send results to WhyLabs using the WhyProfileSession's configurations.

        Users must specify the organization ID, the model ID and the API key.

        You can specify via WHYLABS_ORG_ID, WHYLABS_MODEL_ID and WHYLABS_API_KEY environment variables as well.
        :param dt: the datetime of the dataset. Default to the current time
        :param org_id: the WhyLabs organization ID. Defaults to WHYLABS_ORG_ID environment variable
        :param model_id: the model or dataset ID. Defaults to WHYLABS_MODEL_ID environment variable
        :param api_key: the whylabs API key. Defaults to WHYLABS_API_KEY environment variable
        :param endpoint: theh API endpiont
        """
        if dt is not None:
            timestamp_ms = int(dt.timestamp() * 1000)
        else:
            timestamp_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        if org_id is None:
            org_id = os.environ.get('WHYLABS_ORG_ID')
            if org_id is None:
                raise RuntimeError('Please specify the org ID')
        if model_id is None:
            model_id = os.environ.get('WHYLABS_MODEL_ID')
            if model_id is None:
                raise RuntimeError('Please specify the model ID')
        if api_key is None:
            api_key = os.environ.get('WHYLABS_API_KEY')
            if api_key is None:
                raise RuntimeError('Please specify the API key')

        j_session = self._create_j_session()
        j_session.log(timestamp_ms, org_id, model_id, api_key, endpoint)


def new_profiling_session(df: DataFrame, name: str, time_column: Optional[str] = None):
    if time_column is None:
        return WhyProfileSession(dataframe=df, name=name)
    else:
        return WhyProfileSession(dataframe=df, name=name, time_column=time_column)
