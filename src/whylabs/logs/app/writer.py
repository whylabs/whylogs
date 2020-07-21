from typing import List

from whylabs.logs.core import DatasetProfile


class Writer:
    def __init__(self, tpe: str, output: str, path: str, formats: List[str]):
        self.type = tpe
        self.output = output
        self.path = path
        self.formats = formats

    def write(self, profile: DatasetProfile):
        pass
