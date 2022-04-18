import typing

from whylogs_v1.api.profiling_results import ProfilingResults
from whylogs_v1.core import DatasetProfile, DatasetProfileView, DatasetSchema
from whylogs_v1.core.stubs import pd


class BasicCache(object):
    _cache: typing.Dict[DatasetSchema, DatasetProfile] = {}

    def get(self, schema: DatasetSchema) -> DatasetProfile:
        candidate = self._cache.get(schema)
        if candidate is None:
            candidate = DatasetProfile(schema=schema)
            self._cache[schema] = candidate
        return candidate


class Logger(object):
    _cache = BasicCache()

    def log(
        self,
        *,
        pandas: typing.Optional[pd.DataFrame] = None,
        row: typing.Optional[typing.Dict[str, typing.Any]] = None,
        schema: typing.Optional[DatasetSchema] = None,
    ) -> ProfilingResults:
        if schema is None:
            schema = DatasetSchema()
        profile = self._cache.get(schema)

        profile.track(pandas=pandas, row=row)

        return ProfilingResults(profile=profile)


_INSTANCE = Logger()


def log(
    *,
    pandas: typing.Optional[pd.DataFrame] = None,
    row: typing.Optional[typing.Dict[str, typing.Any]] = None,
    schema: typing.Optional[DatasetSchema] = None,
) -> ProfilingResults:
    return _INSTANCE.log(pandas=pandas, row=row, schema=schema)


def read(path: str) -> DatasetProfileView:
    return DatasetProfile.read(path)


def write(profile: DatasetProfile, base_dir: str) -> None:
    return profile.write(base_dir)
