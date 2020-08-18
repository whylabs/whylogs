import os
from abc import ABC, abstractmethod
from typing import List, Tuple

from google.protobuf.message import Message

from whylabs.logs.app.config import WriterConfig
from whylabs.logs.core import DatasetProfile, datasetprofile
from whylabs.logs.util.protobuf import message_to_json


class Writer(ABC):
    def __init__(self, output_path: str, formats: List[str]):
        if "all" in formats:
            formats = ["json", "flat", "protobuf"]

        self.formats = formats
        self.output_path = output_path

    @abstractmethod
    def write(self, profile: DatasetProfile):
        pass


def _write_json(path: str, profile: DatasetProfile):
    summary = profile.to_summary()
    with open(os.path.join(path, "whylogs.json"), "wt") as f:
        f.write(message_to_json(summary))


def _write_flat(path: str, profile: DatasetProfile):
    summary = profile.to_summary()
    flat_summary: dict = datasetprofile.flatten_summary(summary)
    # TODO: use absolute path when writing out data
    os.chdir(path)
    datasetprofile.write_flat_dataset_summary(flat_summary, "summary")


def _write_protobuf(path: str, profile: DatasetProfile):
    protobuf: Message = profile.to_protobuf()
    with open(os.path.join(path, "protobuf.bin"), "wb") as f:
        f.write(protobuf.SerializeToString())


class LocalWriter(Writer):
    def __init__(self, output_path: str, formats: List[str]):
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Path does not exist: {output_path}")
        super().__init__(output_path, formats)

    def write(self, profile: DatasetProfile):
        session_timestamp = round(profile.session_timestamp.timestamp() * 1000)
        profile_session_path = os.path.join(
            self.output_path, profile.name, f"{session_timestamp}"
        )
        os.makedirs(profile_session_path, exist_ok=True)
        for fmt in self.formats:
            if fmt == "json":
                _write_json(profile_session_path, profile)
            elif fmt == "flat":
                _write_flat(profile_session_path, profile)
            elif fmt == "protobuf":
                _write_protobuf(profile_session_path, profile)
            else:
                raise ValueError(f"Unsupported format: {fmt}")


class S3Writer(Writer):
    def __init__(self, output_path: str, formats: List[str]):
        if not os.path.exists(output_path):
            raise FileNotFoundError("")
        super().__init__(output_path, formats)

    def write(self, profile: DatasetProfile):
        pass


def writer_from_config(config: WriterConfig):
    abs_path = os.path.abspath(config.output_path)
    if not os.path.exists(abs_path):
        os.makedirs(abs_path, exist_ok=True)

    if config.type == "local":
        return LocalWriter(config.output_path, config.formats)
    elif config.type == "s3":
        return S3Writer(config.output_path, config.formats)
    else:
        raise ValueError(f"Unknown writer type: {config.type}")
