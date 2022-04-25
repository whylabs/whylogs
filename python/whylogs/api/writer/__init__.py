from whylogs.api.writer.writer import Writer


class Writers:
    @classmethod
    def get(cls, name: str) -> "Writer":
        if name == "local":
            from whylogs.api.writer.local import LocalWriter

            return LocalWriter()
        elif name == "whylabs":
            from whylogs.api.writer.whylabs import WhyLabsWriter

            return WhyLabsWriter()
        elif name == "s3":
            from whylogs.api.writer.s3 import S3Writer

            return S3Writer()
        elif name == "mlflow":
            from whylogs.api.writer.mlflow import MlflowWriter

            return MlflowWriter()

        raise ValueError(f"Unrecognized writer: {name}")
