import json
import os
from abc import ABC, abstractmethod
from typing import List

from google.protobuf.message import Message

from whylogs.app.output_formats import OutputFormat
from whylogs.core import DatasetProfile

from ..core.datasetprofile import (
    flatten_dataset_frequent_numbers,
    flatten_dataset_frequent_strings,
    flatten_dataset_histograms,
    get_dataset_frame,
)
from ..util.protobuf import message_to_json
from .config import WriterConfig


class Writer(ABC):
    def __init__(self, output_path: str, formats: List[str]):
        self.formats = []
        if "all" in formats:
            for fmt in OutputFormat.__members__.values():
                self.formats.append(fmt)
        else:
            for fmt in formats:
                fmt_ = OutputFormat[fmt]
                if fmt_ is None:
                    raise ValueError("Unsupported format: {0}".format(fmt))
                else:
                    self.formats.append(fmt_)

        self.output_path = output_path

    @abstractmethod
    def write(self, profile: DatasetProfile):
        pass


def _write_json(path: str, profile: DatasetProfile):
    summary = profile.to_summary()
    with open(os.path.join(path, "whylogs.json"), "wt") as f:
        f.write(message_to_json(summary))


def _write_flat(path: str, profile: DatasetProfile, indent: int = 4):
    """
    Write output data for flat format

    Parameters
    ----------
    path  the base path for WhyLogs output
    profile the dataset profile to output
    indent the JSON indentation. Default is 4
    -------

    """
    summary = profile.to_summary()

    flat_output_dir = os.path.join(path, "flat")
    os.makedirs(flat_output_dir, exist_ok=True)

    # write output data
    summary_df = get_dataset_frame(summary)
    summary_df.to_csv(os.path.join(flat_output_dir, "summary.csv"), index=False)

    with open(os.path.join(flat_output_dir, "historgram.json"), "wt") as f:
        hist = flatten_dataset_histograms(summary)
        json.dump(hist, f, indent=indent)
    with open(os.path.join(flat_output_dir, "frequent_strings.json"), "wt") as f:
        frequent_strings = flatten_dataset_frequent_strings(summary)
        json.dump(frequent_strings, f, indent=indent)
    with open(os.path.join(flat_output_dir, "frequent_strings.json"), "wt") as f:
        frequent_numbers = flatten_dataset_frequent_numbers(summary)
        json.dump(frequent_numbers, f, indent=indent)


def _write_protobuf(path: str, profile: DatasetProfile):
    protobuf: Message = profile.to_protobuf()
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
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
            if fmt == OutputFormat.json:
                _write_json(profile_session_path, profile)
            elif fmt == OutputFormat.flat:
                _write_flat(profile_session_path, profile)
            elif fmt == OutputFormat.protobuf:
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
