from typing import Any

from whylogs.api.writer.writer import Writer


class Writers:
    @staticmethod
    def get(name: str, **kwargs: Any) -> "Writer":
        if name == "local":
            from whylogs.api.writer.local import LocalWriter

            return LocalWriter(**kwargs)  # type: ignore
        elif name == "whylabs":
            from whylogs.api.writer.whylabs import WhyLabsWriter

            return WhyLabsWriter(**kwargs)  # type: ignore
        elif name == "s3":
            from whylogs.api.writer.s3 import S3Writer

            return S3Writer(**kwargs)  # type: ignore
        elif name == "mlflow":
            from whylogs.api.writer.mlflow import MlflowWriter

            return MlflowWriter(**kwargs)  # type: ignore
        raise ValueError(f"Unrecognized writer: {name}")
