from typing import Any, Dict, Optional

from whylogs.api.profiling_results import ProfilingResults
from whylogs.core import DatasetProfile, DatasetProfileView, DatasetSchema
from whylogs.core.stubs import pd


class BasicCache(object):
    _cache: Dict[DatasetSchema, DatasetProfile] = {}

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
        pandas: Optional[pd.DataFrame] = None,
        row: Optional[Dict[str, Any]] = None,
        schema: Optional[DatasetSchema] = None,
    ) -> ProfilingResults:
        if schema is None:
            schema = DatasetSchema()

        if schema.schema_based_automerge:
            profile = self._cache.get(schema)
        else:
            profile = DatasetProfile(schema=schema)
        profile.track(pandas=pandas, row=row)

        return ProfilingResults(profile=profile)


_INSTANCE = Logger()


def log(
    *,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
    schema: Optional[DatasetSchema] = None,
) -> ProfilingResults:
    return _INSTANCE.log(pandas=pandas, row=row, schema=schema)


def read(path: str) -> DatasetProfileView:
    return DatasetProfile.read(path)


def write(profile: DatasetProfile, base_dir: str) -> None:
    return profile.write(base_dir)
