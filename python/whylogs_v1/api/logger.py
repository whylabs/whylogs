from typing import Any, Dict, Optional, Union

from whylogs_v1.core import DatasetProfile, DatasetProfileView, DatasetSchema
from whylogs_v1.core.stubs import pd


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
        data: Union[pd.DataFrame, Dict[str, Any]],
        schema: Optional[DatasetSchema] = None,
    ) -> DatasetProfile:
        if schema is None:
            schema = DatasetSchema()
        prof = self._cache.get(schema)

        if isinstance(data, pd.DataFrame):
            prof.track(data)
            return prof

        raise NotImplementedError


_INSTANCE = Logger()


def log(data: Union[pd.DataFrame, Dict[str, Any]], schema: Optional[DatasetSchema] = None) -> DatasetProfile:
    return _INSTANCE.log(data, schema)


def read(path: str) -> DatasetProfileView:
    return DatasetProfile.read(path)


def write(profile: DatasetProfile, base_dir: str) -> None:
    return profile.write(base_dir)
