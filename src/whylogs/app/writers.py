"""
Classes for writing whylogs output
"""
import json
import os
import tempfile
import typing
from abc import ABC, abstractmethod
from logging import getLogger
from string import Template
from typing import List, Optional

import boto3
from smart_open import open

from whylogs.app.output_formats import OutputFormat
from whylogs.core import DatasetProfile
from whylogs.core.flatten_datasetprofile import (
    flatten_dataset_frequent_strings,
    flatten_dataset_histograms,
    get_dataset_frame,
)

from ..util import time
from ..util.protobuf import message_to_json
from .config import TransportParameterConfig, WriterConfig, WriterType
from .utils import async_wrap

DEFAULT_PATH_TEMPLATE = "$name/$session_id"
DEFAULT_FILENAME_TEMPLATE = "dataset_profile"

logger = getLogger(__name__)


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
        transport_params: Optional[TransportParameterConfig] = None,
    ):

        if path_template is None:
            path_template = DEFAULT_PATH_TEMPLATE
        if filename_template is None:
            filename_template = DEFAULT_FILENAME_TEMPLATE
        self.path_template = Template(path_template)
        self.filename_template = Template(filename_template)
        self.transport_params = transport_params
        self._pending_threads = []
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

    def close(self):
        for t in self._pending_threads:
            t.join()
        self._pending_threads.clear()

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

    def file_name(self, profile: DatasetProfile, file_extension: str, rotation_suffix: Optional[str] = None):
        """
        For a given DatasetProfile, generate an output filename based on the
        templating defined in `self.filename_template`
        """
        kwargs = self.template_params(profile)
        file_name = self.filename_template.substitute(**kwargs)
        if rotation_suffix is not None:
            return file_name + rotation_suffix + file_extension
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

    def write(self, profile: DatasetProfile, rotation_suffix: Optional[str] = None):
        """
        Write a dataset profile to disk
        """
        t = async_wrap(self._do_write, profile, rotation_suffix)
        self._pending_threads.append(t)

    def _do_write(self, profile, rotation_suffix: Optional[str] = None, **kwargs):

        for fmt in self.formats:
            if fmt == OutputFormat.json:
                self._write_json(
                    profile,
                    rotation_suffix,
                )
            elif fmt == OutputFormat.flat:
                self._write_flat(profile, rotation_suffix=rotation_suffix)
            elif fmt == OutputFormat.protobuf:
                self._write_protobuf(profile, rotation_suffix)
            else:
                raise ValueError(f"Unsupported format: {fmt}")

    def _write_json(self, profile: DatasetProfile, rotation_suffix: Optional[str] = None):
        """
        Write a JSON summary of the dataset profile to disk
        """
        path = self.ensure_path(os.path.join(self.path_suffix(profile), "json"))

        output_file = os.path.join(path, self.file_name(profile, ".json", rotation_suffix))

        path = os.path.join(self.output_path, self.path_suffix(profile))
        os.makedirs(path, exist_ok=True)
        # TODO: only calculate this summary once.  No need to calculate it for
        # _write_flat() as well
        summary = profile.to_summary()
        with open(output_file, "wt") as f:
            f.write(message_to_json(summary))

    def _write_flat(self, profile: DatasetProfile, indent: int = 4, rotation_suffix: Optional[str] = None):
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
        summary_df.to_csv(os.path.join(flat_table_path, self.file_name(profile, ".csv", rotation_suffix)), index=False)

        _suffix = rotation_suffix or ""
        json_flat_file = self.file_name(profile, ".json")

        frequent_strings_path = self.ensure_path(os.path.join(self.path_suffix(profile), f"frequent_strings{_suffix}"))
        with open(os.path.join(frequent_strings_path, json_flat_file), "wt") as f:
            frequent_strings = flatten_dataset_frequent_strings(summary)
            json.dump(frequent_strings, f, indent=indent)

        histogram_path = self.ensure_path(os.path.join(self.path_suffix(profile), f"histogram{_suffix}"))
        with open(os.path.join(histogram_path, json_flat_file), "wt") as f:
            histogram = flatten_dataset_histograms(summary)
            json.dump(histogram, f, indent=indent)

    def _write_protobuf(self, profile: DatasetProfile, rotation_suffix: Optional[str] = None):
        """
        Write a protobuf serialization of the DatasetProfile to disk
        """
        path = self.ensure_path(os.path.join(self.path_suffix(profile), "protobuf"))

        with open(os.path.join(path, self.file_name(profile, ".bin", rotation_suffix)), "wb") as f:
            f.write(profile.serialize_delimited())

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
        t = async_wrap(self._do_write, profile, rotation_suffix)
        self._pending_threads.append(t)

    def _do_write(self, profile, rotation_suffix: str = None, **kwargs):
        if self.transport_params:
            session = boto3.Session(
                endpoint_url=self.transport_params.endpoint_url,
                aws_access_key_id=self.transport_params.aws_access_key_id,
                aws_secret_access_key=self.transport_params.aws_secret_access_key,
                region_name=self.transport_params.region_name,
                verify=self.transport_params.verify,
            )
            transport_params = {"client": session.client("s3")}
        else:
            session = boto3.Session(**kwargs)
            transport_params = {"client": session.client("s3")}

        for fmt in self.formats:
            if fmt == OutputFormat.json:
                self._write_json(profile, rotation_suffix, transport_params=transport_params)
            elif fmt == OutputFormat.flat:
                self._write_flat(profile, rotation_suffix=rotation_suffix, transport_params=transport_params)
            elif fmt == OutputFormat.protobuf:
                self._write_protobuf(profile, rotation_suffix, transport_params=transport_params)
            else:
                raise ValueError(f"Unsupported format: {fmt}")

    def _write_json(self, profile: DatasetProfile, rotation_suffix: Optional[str] = None, transport_params: Optional[dict] = None):
        """
        Write a dataset profile JSON summary to disk
        """
        output_file = os.path.join(
            self.output_path,
            self.path_suffix(profile),
            "json",
            self.file_name(profile, ".json", rotation_suffix),
        )

        summary = profile.to_summary()
        with open(output_file, "wt", transport_params=transport_params) as f:
            f.write(message_to_json(summary))

    def _write_flat(self, profile: DatasetProfile, indent: int = 4, rotation_suffix: Optional[str] = None, transport_params: Optional[dict] = None):
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
        with open(os.path.join(flat_table_path, self.file_name(profile, ".csv", rotation_suffix)), "wt", transport_params=transport_params) as f:
            summary_df.to_csv(f, index=False)

        json_flat_file = self.file_name(profile, ".json")
        _suffix = rotation_suffix or ""

        frequent_strings_path = os.path.join(self.output_path, self.path_suffix(profile), f"frequent_strings{_suffix}")
        with open(os.path.join(frequent_strings_path, json_flat_file), "wt", transport_params=transport_params) as f:
            frequent_strings = flatten_dataset_frequent_strings(summary)
            json.dump(frequent_strings, f, indent=indent)

        histogram_path = os.path.join(self.output_path, self.path_suffix(profile), f"histogram{_suffix}")

        with open(os.path.join(histogram_path, json_flat_file), "wt", transport_params=transport_params) as f:
            histogram = flatten_dataset_histograms(summary)
            json.dump(histogram, f, indent=indent)

    def _write_protobuf(self, profile: DatasetProfile, rotation_suffix: Optional[str] = None, transport_params: Optional[dict] = None):
        """
        Write a datasetprofile protobuf serialization to S3
        """
        path = os.path.join(self.output_path, self.path_suffix(profile), "protobuf")

        with open(os.path.join(path, self.file_name(profile, ".bin", rotation_suffix)), "wb", transport_params=transport_params) as f:
            f.write(profile.serialize_delimited())


class MlFlowWriter(Writer):
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
        Write a dataset profile to MLFlow path
        """

        self._write_protobuf(profile, rotation_suffix)

    @staticmethod
    def _write_protobuf(profile: DatasetProfile, rotation_suffix: str = None, **kwargs):
        """
        Write a protobuf the dataset profile to disk in binary format to MlFlow
        """
        import mlflow

        # import whylogs.mlflow.patcher as patcher
        name = profile.name
        tmp_dir = tempfile.mkdtemp()
        logger.debug("Using tmp dir: %s", tmp_dir)
        dataset_dir = name or "default"
        output_dir = os.path.join(tmp_dir, dataset_dir)
        os.makedirs(output_dir, exist_ok=True)
        output = os.path.join(output_dir, "profile.bin")
        logger.debug("Writing logger %s's data to %s", output, f"whylogs/{dataset_dir}")
        profile.write_protobuf(output)
        mlflow.log_artifact(output, artifact_path=f"whylogs/{dataset_dir}")
        logger.debug("Successfully uploaded logger %s data to MLFlow", name)


class WhyLabsWriter(Writer):
    def __init__(self, output_path="", formats=None):
        if formats is None:
            formats = []
        super().__init__(output_path=output_path, formats=formats)

    def write(self, profile: DatasetProfile, rotation_suffix: str = None):
        """
        Write a dataset profile to WhyLabs
        """
        t = async_wrap(self._write_protobuf, profile)
        self._pending_threads.append(t)

    @staticmethod
    def _write_protobuf(profile: DatasetProfile):
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
    if config.type == WriterType.local.name:
        abs_path = os.path.abspath(config.output_path)
        if not os.path.exists(abs_path):
            os.makedirs(abs_path, exist_ok=True)

        return LocalWriter(
            config.output_path,
            config.formats,
            config.path_template,
            config.filename_template,
        )
    elif config.type == WriterType.s3.name:
        return S3Writer(
            config.output_path,
            config.formats,
            config.path_template,
            config.filename_template,
        )
    elif config.type == WriterType.mlflow.name:
        return MlFlowWriter(
            config.output_path,
            config.formats,
            config.path_template,
            config.filename_template,
        )
    elif config.type == WriterType.whylabs.name:
        return WhyLabsWriter(config.output_path, config.formats)
    else:
        raise ValueError(f"Unknown writer type: {config.type}")
