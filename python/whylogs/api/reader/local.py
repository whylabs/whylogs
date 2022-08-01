import whylogs as why
from whylogs.api.logger.result_set import ResultSet
from whylogs.api.reader.reader import Reader


class LocalReader(Reader):
    def read(path: str) -> ResultSet:
        return why.read(path)
