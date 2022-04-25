from typing import Optional

from whylogs.api.writer import Writer
from whylogs.core import DatasetProfileView


class MlflowWriter(Writer):
    def write(self, profile: DatasetProfileView, dest: Optional[str] = None) -> None:
        pass

    def option(self, param: Optional[str] = None) -> "MlflowWriter":  # type: ignore
        pass
