"""
Classes for writing whylogs output
"""
import json
import os
import typing
from abc import ABC, abstractmethod
from string import Template
from typing import List

from google.protobuf.message import Message
from smart_open import open

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

DEFAULT_PATH_TEMPLATE = "$name/$session_id"
DEFAULT_FILENAME_TEMPLATE = "dataset_profile"


class Writer(ABC):
    """
    Class for writing to disk

    Parameters
    ----------
    output_path : str
        Prefix of where to output files.  A directory for `type = 'local'`,
        or key prefix for `type = 's3'`
    formats : list
        All output formats.
        See :data:`whylogs.app.config.ALL_SUPPORTED_FORMATS`
    path_template : str, optional
        Templatized path output using standard python string templates.
        Variables are accessed via $identifier or ${identifier}.
        See :func:`Writer.template_params` for a list of available identifers.
        Default = :data:`DEFAULT_PATH_TEMPLATE`
    filename_template : str, optional
        Templatized output filename using standardized python string templates.
        Variables are accessed via $identifier or ${identifier}.
        See :func:`Writer.template_params` for a list of available identifers.
        Default = :data:`DEFAULT_FILENAME_TEMPLATE`
    """

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
        self.rotation_suffix = None

    @abstractmethod
    def write(self, profile: DatasetProfile, rotation_suffix: str = None):
        """
        Abstract method to write a dataset profile to disk.  Must be
        implemented
        """

        raise NotImplementedError

    def path_suffix(self, profile: DatasetProfile):
        """
        Generate a path string for an output path from a dataset profile by
        applying the path templating defined in `self.path_template`
        """
        kwargs = self.template_params(profile)
        path = self.path_template.substitute(**kwargs)
        return path

    def file_name(self, profile: DatasetProfile, file_extension: str):
        """
        For a given DatasetProfile, generate an output filename based on the
        templating defined in `self.filename_template`
        """
        kwargs = self.template_params(profile)
        file_name = self.filename_template.substitute(**kwargs)
        if self.rotation_suffix is not None:
            return file_name + self.rotation_suffix + file_extension
        else:
            return file_name + file_extension

    @staticmethod
    def template_params(profile: DatasetProfile) -> dict:
        """
        Return a dictionary of dataset profile metadata which can be used for
        generating templatized variables or paths.

        Parameters
        ----------
        profile : DatasetProfile
            The dataset profile

        Returns
        -------
        params : dict
            Variables which can be substituted into a template string.


        Notes
        -----
        Template params:

        * ``name``: name of the dataset
        * ``session_timestamp``: session time in UTC epoch milliseconds
        * ``dataset_timestamp``: timestamp for the data in UTC epoch ms
        * ``session_id``: Unique identifier for the session
        """
        dataset_timestamp = "batch"
        if profile.dataset_timestamp is not None:
            dataset_timestamp = time.to_utc_ms(profile.dataset_timestamp).__str__()
        return {
            "name": profile.name,
            "session_timestamp": str(time.to_utc_ms(profile.session_timestamp)),
            "dataset_timestamp": dataset_timestamp,
            "session_id": profile.session_id or "missing-session-id",
        }


class LocalWriter(Writer):
    """
    whylogs Writer class that can write to disk.

    See :class:`Writer` for a description of arguments
    """

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

    def write(self, profile: DatasetProfile, rotation_suffix: str = None):
        """
        Write a dataset profile to disk
        """
        self.rotation_suffix = rotation_suffix
        for fmt in self.formats:
            if fmt == OutputFormat.json:
                self._write_json(profile)
            elif fmt == OutputFormat.flat:
                self._write_flat(profile)
            elif fmt == OutputFormat.protobuf:
                self._write_protobuf(profile)
            else:
                raise ValueError(f"Unsupported format: {fmt}")
        self.rotation_suffix = None

    def _write_json(self, profile: DatasetProfile):
        """
        Write a JSON summary of the dataset profile to disk
        """
        path = self.ensure_path(os.path.join(self.path_suffix(profile), "json"))

        output_file = os.path.join(path, self.file_name(profile, ".json"))

        path = os.path.join(self.output_path, self.path_suffix(profile))
        os.makedirs(path, exist_ok=True)
        # TODO: only calculate this summary once.  No need to calculate it for
        # _write_flat() as well
        summary = profile.to_summary()
        with open(output_file, "wt") as f:
            f.write(message_to_json(summary))

    def _write_flat(self, profile: DatasetProfile, indent: int = 4):
        """
        Write output data for flat format

        Parameters
        ----------
        profile : DatasetProfile
            the dataset profile to output
        indent : int
            The JSON indentation to use. Default is 4
        """
        # TODO: only calculate this summary once.  No need to calculate it for
        # _write_json() as well
        summary = profile.to_summary()

        flat_table_path = self.ensure_path(os.path.join(self.path_suffix(profile), "flat_table"))
        summary_df = get_dataset_frame(summary)
        summary_df.to_csv(os.path.join(flat_table_path, self.file_name(profile, ".csv")), index=False)

        frequent_numbers_path = self.ensure_path(os.path.join(self.path_suffix(profile), "freq_numbers"))
        json_flat_file = self.file_name(profile, ".json")
        with open(os.path.join(frequent_numbers_path, json_flat_file), "wt") as f:
            hist = flatten_dataset_frequent_numbers(summary)
            json.dump(hist, f, indent=indent)

        frequent_strings_path = self.ensure_path(os.path.join(self.path_suffix(profile), "frequent_strings"))
        with open(os.path.join(frequent_strings_path, json_flat_file), "wt") as f:
            frequent_strings = flatten_dataset_frequent_strings(summary)
            json.dump(frequent_strings, f, indent=indent)

        histogram_path = self.ensure_path(os.path.join(self.path_suffix(profile), "histogram"))
        with open(os.path.join(histogram_path, json_flat_file), "wt") as f:
            histogram = flatten_dataset_histograms(summary)
            json.dump(histogram, f, indent=indent)

    def _write_protobuf(self, profile: DatasetProfile):
        """
        Write a protobuf serialization of the DatasetProfile to disk
        """
        path = self.ensure_path(os.path.join(self.path_suffix(profile), "protobuf"))

        protobuf: Message = profile.to_protobuf()

        with open(os.path.join(path, self.file_name(profile, ".bin")), "wb") as f:
            f.write(protobuf.SerializeToString())

    def ensure_path(self, suffix: str, addition_part: typing.Optional[str] = None) -> str:
        """
        Ensure that a path exists, creating it if not
        """
        path = os.path.join(self.output_path, suffix)
        if addition_part is not None:
            path = os.path.join(path, addition_part)
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        return path


class S3Writer(Writer):
    """
    whylogs Writer class that can write to S3.

    See :class:`Writer` for a description of arguments
    """

    def __init__(
        self,
        output_path: str,
        formats: List[str],
        path_template: str = None,
        filename_template: str = None,
    ):
        super().__init__(output_path, formats, path_template, filename_template)

    def write(self, profile: DatasetProfile, rotation_suffix: str = None):
        """
        Write a dataset profile to S3
        """
        self.rotation_suffix = rotation_suffix

        for fmt in self.formats:
            if fmt == OutputFormat.json:
                self._write_json(profile)
            elif fmt == OutputFormat.flat:
                self._write_flat(profile)
            elif fmt == OutputFormat.protobuf:
                self._write_protobuf(profile)
            else:
                raise ValueError(f"Unsupported format: {fmt}")
        self.rotation_suffix = None

    def _write_json(self, profile: DatasetProfile):
        """
        Write a dataset profile JSON summary to disk
        """
        output_file = os.path.join(
            self.output_path,
            self.path_suffix(profile),
            "json",
            self.file_name(profile, ".json"),
        )

        summary = profile.to_summary()
        with open(output_file, "wt") as f:
            f.write(message_to_json(summary))

    def _write_flat(self, profile: DatasetProfile, indent: int = 4):
        """
        Write output data for flat format

        Parameters
        ----------
        profile : DatasetProfile
            the dataset profile to output
        indent : int
            The JSON indentation to use. Default is 4
        """
        summary = profile.to_summary()

        flat_table_path = os.path.join(self.output_path, self.path_suffix(profile), "flat_table")
        summary_df = get_dataset_frame(summary)
        with open(os.path.join(flat_table_path, self.file_name(profile, ".csv")), "wt") as f:
            summary_df.to_csv(f, index=False)

        json_flat_file = self.file_name(profile, ".json")

        frequent_numbers_path = os.path.join(self.output_path, self.path_suffix(profile), "freq_numbers")
        with open(os.path.join(frequent_numbers_path, json_flat_file), "wt") as f:
            hist = flatten_dataset_histograms(summary)
            json.dump(hist, f, indent=indent)

        frequent_strings_path = os.path.join(self.output_path, self.path_suffix(profile), "frequent_strings")
        with open(os.path.join(frequent_strings_path, json_flat_file), "wt") as f:
            frequent_strings = flatten_dataset_frequent_strings(summary)
            json.dump(frequent_strings, f, indent=indent)

        histogram_path = os.path.join(self.output_path, self.path_suffix(profile), "histogram")

        with open(os.path.join(histogram_path, json_flat_file), "wt") as f:
            histogram = flatten_dataset_histograms(summary)
            json.dump(histogram, f, indent=indent)

    def _write_protobuf(self, profile: DatasetProfile):
        """
        Write a datasetprofile protobuf serialization to S3
        """
        path = os.path.join(self.output_path, self.path_suffix(profile), "protobuf")

        protobuf: Message = profile.to_protobuf()

        with open(os.path.join(path, self.file_name(profile, ".bin")), "wb") as f:
            f.write(protobuf.SerializeToString())


class WhyLabsWriter(Writer):
    def write(self, profile: DatasetProfile, rotation_suffix: str = None):
        """
        Write a dataset profile to WhyLabs
        """
        self.rotation_suffix = rotation_suffix
        self._write_protobuf(profile)
        self.rotation_suffix = None

    def _write_protobuf(self, profile: DatasetProfile):
        """
        Write a protobuf profile to WhyLabs
        """
        from whylogs.whylabs_client.wrapper import upload_profile

        upload_profile(profile)


def writer_from_config(config: WriterConfig):
    """
    Construct a whylogs `Writer` from a `WriterConfig`

    Returns
    -------
    writer : Writer
        whylogs writer
    """
    if config.type == "local":
        abs_path = os.path.abspath(config.output_path)
        if not os.path.exists(abs_path):
            os.makedirs(abs_path, exist_ok=True)

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
    elif config.type == "whylabs":
        if config.data_collection_consent is not True:
            raise ValueError(f"Writer of type {config.type} requires data_collection_consent parameter to be set to True")
        return WhyLabsWriter()
    else:
        raise ValueError(f"Unknown writer type: {config.type}")
