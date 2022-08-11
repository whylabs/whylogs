from typing import Optional

from whylogs.api.logger.result_set import ResultSet
from whylogs.api.reader.reader import Reader


class LocalReader(Reader):
    def __init__(self, path: Optional[str] = None):
        self.path = path

    def read(self, path: Optional[str] = None, **kwargs) -> ResultSet:
        file_path = path or self.path
        if file_path is None:
            raise ValueError("You must define a path to read your file from!")
        return self.get_file_from_path(path=file_path)

    def option(self, path: Optional[str]) -> "LocalReader":
        if path is not None:
            self.path = path
        return self
