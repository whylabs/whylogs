from typing import Any, Dict, List, Optional

from whylogs.api.logger.logger import Logger
from whylogs.core import DatasetProfile, DatasetSchema
from whylogs.core.stubs import pd


class TransientLogger(Logger):
    def __init__(self, schema: Optional[DatasetSchema] = None):
        super(TransientLogger, self).__init__(schema)

    def _get_matching_profiles(
        self, obj: Any = None, *, pandas: Optional[pd.DataFrame] = None, row: Optional[Dict[str, Any]] = None
    ) -> List[DatasetProfile]:
        return [DatasetProfile(schema=self._schema)]

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass
