from typing import Optional

from whylogs.api.logger.result_set import ResultSet
from whylogs.api.reader.reader import Reader


class LocalReader(Reader):
    def __init__(self, path: Optional[str] = None) -> None:
        self.path = path

    def read(self, **kwargs) -> ResultSet:
        return self.get_file_from_path(path=self.path)

    @classmethod
    def option(cls, path: Optional[str]):
        if path is not None:
            cls.path = path
