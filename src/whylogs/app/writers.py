import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

import typing

import s3fs
from google.protobuf.message import Message

from whylogs.app.output_formats import OutputFormat
from whylogs.core import DatasetProfile

from ..core.datasetprofile import (
    flatten_dataset_frequent_numbers,
    flatten_dataset_frequent_strings,
    flatten_dataset_histograms,
    get_dataset_frame,
)
from ..util import time
from ..util.protobuf import message_to_json
from .config import WriterConfig
from string import Template

DEFAULT_PATH_TEMPLATE = "$name/$session_id"
DEFAULT_FILENAME_TEMPLATE = "dataset_profile"


class Writer(ABC):
    def __init__(
        self,
        output_path: str,
        formats: List[str],
        path_template: typing.Optional[str] = None,
        filename_template: typing.Optional[str] = None,
    ):
        if path_template is None:
            path_template = DEFAULT_PATH_TEMPLATE
        if filename_template is None:
            filename_template = DEFAULT_FILENAME_TEMPLATE
        self.path_template = Template(path_template)
        self.filename_template = Template(filename_template)

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

    def path_suffix(self, profile: DatasetProfile):
        kwargs = self.get_kwargs(profile)
        path = self.path_template.substitute(**kwargs)
        return path

    def file_name(self, profile: DatasetProfile, file_extension: str):
        kwargs = self.get_kwargs(profile)
        file_name = self.filename_template.substitute(**kwargs)
        return file_name + file_extension

    @staticmethod
    def get_kwargs(profile: DatasetProfile) -> dict:
        dataset_timestamp = "batch"
        if profile.data_timestamp is not None:
            dataset_timestamp = time.to_utc_ms(profile.data_timestamp).__str__()
        return {
            "name": profile.name,
            "session_timestamp": str(time.to_utc_ms(profile.session_timestamp)),
            "dataset_timestamp": dataset_timestamp,
            "session_id": profile.session_id or "missing-session-id",
        }


class LocalWriter(Writer):
    def __init__(
        self,
        output_path: str,
        formats: List[str],
        path_template: str,
        filename_template: str,
    ):
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Path does not exist: {output_path}")
        super().__init__(output_path, formats, path_template, filename_template)

    def write(self, profile: DatasetProfile):
        for fmt in self.formats:
            if fmt == OutputFormat.json:
                self._write_json(profile)
            elif fmt == OutputFormat.flat:
                self._write_flat(profile)
            elif fmt == OutputFormat.protobuf:
                self._write_protobuf(profile)
            else:
                raise ValueError(f"Unsupported format: {fmt}")

    def _write_json(self, profile: DatasetProfile):
        path = self.ensure_path(os.path.join(self.path_suffix(profile), "json"))

        output_file = os.path.join(path, self.file_name(profile, "json"))

        path = os.path.join(self.output_path, self.path_suffix(profile))
        os.makedirs(path, exist_ok=True)

        summary = profile.to_summary()
        with open(output_file, "wt") as f:
            f.write(message_to_json(summary))

    def _write_flat(self, profile: DatasetProfile, indent: int = 4):
        """
        Write output data for flat format

        Parameters
        ----------
        profile the dataset profile to output
        indent the JSON indentation. Default is 4
        -------

        """
        summary = profile.to_summary()

        flat_table_path = self.ensure_path(
            os.path.join(self.path_suffix(profile), "flat_table")
        )
        summary_df = get_dataset_frame(summary)
        summary_df.to_csv(
            os.path.join(flat_table_path, self.file_name(profile, ".csv")), index=False
        )

        frequent_numbers_path = self.ensure_path(
            os.path.join(self.path_suffix(profile), "freq_numbers")
        )
        json_flat_file = self.file_name(profile, ".json")
        with open(os.path.join(frequent_numbers_path, json_flat_file), "wt") as f:
            hist = flatten_dataset_histograms(summary)
            json.dump(hist, f, indent=indent)

        frequent_strings_path = self.ensure_path(
            os.path.join(self.path_suffix(profile), "frequent_strings")
        )
        with open(os.path.join(frequent_strings_path, json_flat_file), "wt") as f:
            frequent_strings = flatten_dataset_frequent_strings(summary)
            json.dump(frequent_strings, f, indent=indent)

        histogram_path = self.ensure_path(
            os.path.join(self.path_suffix(profile), "histogram")
        )
        with open(os.path.join(histogram_path, json_flat_file), "wt") as f:
            histogram = flatten_dataset_histograms(summary)
            json.dump(histogram, f, indent=indent)

    def _write_protobuf(self, profile: DatasetProfile):
        path = self.ensure_path(os.path.join(self.path_suffix(profile), "protobuf"))

        protobuf: Message = profile.to_protobuf()

        with open(os.path.join(path, self.file_name(profile, ".bin")), "wb") as f:
            f.write(protobuf.SerializeToString())

    def ensure_path(
        self, suffix: str, addition_part: typing.Optional[str] = None
    ) -> str:
        path = os.path.join(self.output_path, suffix)
        if addition_part is not None:
            path = os.path.join(path, addition_part)
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        return path


class S3Writer(Writer):
    def __init__(
        self,
        output_path: str,
        formats: List[str],
        path_template: str = None,
        filename_template: str = None,
    ):
        super().__init__(output_path, formats, path_template, filename_template)
        self.fs = s3fs.S3FileSystem(anon=False)

    def write(self, profile: DatasetProfile):
        for fmt in self.formats:
            if fmt == OutputFormat.json:
                self._write_json(profile)
            elif fmt == OutputFormat.flat:
                self._write_flat(profile)
            elif fmt == OutputFormat.protobuf:
                self._write_protobuf(profile)
            else:
                raise ValueError(f"Unsupported format: {fmt}")

    def _write_json(self, profile: DatasetProfile):
        output_file = os.path.join(
            self.output_path,
            self.path_suffix(profile),
            "json",
            self.file_name(profile, "json"),
        )

        summary = profile.to_summary()
        with self.fs.open(output_file, "wb") as f:
            f.write(message_to_json(summary))

    def _write_flat(self, profile: DatasetProfile, indent: int = 4):
        """
        Write output data for flat format

        Parameters
        ----------
        profile the dataset profile to output
        indent the JSON indentation. Default is 4
        -------

        """
        summary = profile.to_summary()

        flat_table_path = os.path.join(
            self.output_path, self.path_suffix(profile), "flat_table"
        )
        summary_df = get_dataset_frame(summary)
        with self.fs.open(
            os.path.join(flat_table_path, self.file_name(profile, ".csv")), "wt"
        ) as f:
            summary_df.to_csv(f, index=False)

        json_flat_file = self.file_name(profile, ".json")

        frequent_numbers_path = os.path.join(
            self.output_path, self.path_suffix(profile), "freq_numbers"
        )
        with self.fs.open(
            os.path.join(frequent_numbers_path, json_flat_file), "wt"
        ) as f:
            hist = flatten_dataset_histograms(summary)
            json.dump(hist, f, indent=indent)

        frequent_strings_path = os.path.join(
            self.output_path, self.path_suffix(profile), "frequent_strings"
        )
        with self.fs.open(
            os.path.join(frequent_strings_path, json_flat_file), "wt"
        ) as f:
            frequent_strings = flatten_dataset_frequent_strings(summary)
            json.dump(frequent_strings, f, indent=indent)

        histogram_path = os.path.join(
            self.output_path, self.path_suffix(profile), "histogram"
        )

        with self.fs.open(os.path.join(histogram_path, json_flat_file), "wt") as f:
            histogram = flatten_dataset_histograms(summary)
            json.dump(histogram, f, indent=indent)

    def _write_protobuf(self, profile: DatasetProfile):
        path = os.path.join(self.output_path, self.path_suffix(profile), "protobuf")

        protobuf: Message = profile.to_protobuf()

        with self.fs.open(
            os.path.join(path, self.file_name(profile, ".bin")), "wb"
        ) as f:
            f.write(protobuf.SerializeToString())


def writer_from_config(config: WriterConfig):
    abs_path = os.path.abspath(config.output_path)
    if not os.path.exists(abs_path):
        os.makedirs(abs_path, exist_ok=True)

    if config.type == "local":
        return LocalWriter(
            config.output_path,
            config.formats,
            config.path_template,
            config.filename_template,
        )
    elif config.type == "s3":
        return S3Writer(
            config.output_path,
            config.formats,
            config.path_template,
            config.filename_template,
        )
    else:
        raise ValueError(f"Unknown writer type: {config.type}")
