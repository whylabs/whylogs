from typing import Any

from whylogs.api.reader.reader import Reader


class Readers:
    @staticmethod
    def get(name: str, **kwargs: Any) -> "Reader":
        if name == "local":
            from whylogs.api.reader.local import LocalReader

            return LocalReader(**kwargs)
        elif name == "s3":
            from whylogs.api.reader.s3 import S3Reader

            return S3Reader(**kwargs)

        raise ValueError(f"Unrecognized reader: {name}")
