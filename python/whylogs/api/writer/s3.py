from typing import Optional

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import T
from whylogs.core import DatasetProfileView


class S3Writer(Writer):
    def write(self, profile: DatasetProfileView, dest: Optional[str] = None) -> None:
        pass

    def option(self: T, **kwargs) -> T:  # type: ignore
        pass
