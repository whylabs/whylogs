"""
Classes/functions for configuring the whylogs app

.. autodata:: ALL_SUPPORTED_FORMATS
"""
from logging import getLogger
from typing import Dict, List, Optional

# import typing
import yaml as yaml
from marshmallow import Schema, fields, post_load, validate

from whylogs.app.output_formats import SUPPORTED_OUTPUT_FORMATS

WHYLOGS_YML = ".whylogs.yaml"

ALL_SUPPORTED_FORMATS = ["all"] + SUPPORTED_OUTPUT_FORMATS
"""Supported output formats for whylogs writer configuration"""

SegmentTag = Dict[str, any]
SegmentTags = List[SegmentTag]


class WriterConfig:
    """
    Config for whylogs writers

    See also:

    * :class:`WriterConfigSchema`
    * :class:`whylogs.app.writers.Writer`
    * :func:`whylogs.app.writers.writer_from_config`

    Parameters
    ----------
    type : str
        Destination for the writer output, e.g. 'local' or 's3'
    formats : list
        All output formats.  See :data:`ALL_SUPPORTED_FORMATS`
    output_path : str
        Prefix of where to output files.  A directory for `type = 'local'`,
        or key prefix for `type = 's3'`
    path_template : str, optional
        Templatized path output using standard python string templates.
        Variables are accessed via $identifier or ${identifier}.
        See :func:`whylogs.app.writers.Writer.template_params` for a list of
        available identifers.
        Default = :data:`whylogs.app.writers.DEFAULT_PATH_TEMPLATE`
    filename_template : str, optional
        Templatized output filename using standardized python string templates.
        Variables are accessed via $identifier or ${identifier}.
        See :func:`whylogs.app.writers.Writer.template_params` for a list of
        available identifers.
        Default = :data:`whylogs.app.writers.DEFAULT_FILENAME_TEMPLATE`
    """

    def __init__(
        self,
        type: str,
        formats: List[str],
        output_path: str,
        path_template: Optional[str] = None,
        filename_template: Optional[str] = None,
        data_collection_consent: Optional[bool] = False,
    ):
        self.type = type
        self.formats = formats
        self.output_path = output_path
        self.path_template = path_template
        self.filename_template = filename_template
        self.data_collection_consent = data_collection_consent

    def to_yaml(self, stream=None):
        """
        Serialize this config to YAML

        Parameters
        ----------
        stream
            If None (default) return a string, else dump the yaml into this
            stream.
        """
        dump = WriterConfigSchema().dump(self)
        return yaml.dump(dump, stream)

    @staticmethod
    def from_yaml(stream, **kwargs):
        """
        Load config from yaml

        Parameters
        ----------
        stream : str, file-obj
            String or file-like object to load yaml from

        kwargs
            ignored

        Returns
        -------
        config : `WriterConfig`
            Generated config
        """
        data = yaml.safe_load(stream)
        return WriterConfigSchema().load(data)


class SessionConfig:
    """
    Config for a whylogs session.

    See also :class:`SessionConfigSchema`

    Parameters
    ----------
    project : str
        Project associated with this whylogs session
    pipeline : str
        Name of the associated data pipeline
    writers : list
        A list of `WriterConfig` objects defining writer outputs
    verbose : bool, default=False
        Output verbosity
    with_rotation_time: str, default = None, to rotate profiles with time, takes values of overall rotation interval,
            "s" for seconds
            "m" for minutes
            "h" for hours
            "d" for days

    cache_size: int default =1, sets how many dataprofiles to cache in logger during rotation
    """

    def __init__(
        self,
        project: str,
        pipeline: str,
        writers: List[WriterConfig],
        verbose: bool = False,
        with_rotation_time: str = None,
        cache_size: int = 1,
        report_progress: bool = False,
    ):
        self.project = project
        self.pipeline = pipeline
        self.verbose = verbose
        self.writers = writers
        self.with_rotation_time = with_rotation_time
        self.cache_size = cache_size
        self.report_progress = report_progress

    def to_yaml(self, stream=None):
        """
        Serialize this config to YAML

        Parameters
        ----------
        stream
            If None (default) return a string, else dump the yaml into this
            stream.
        """
        return yaml.dump(SessionConfigSchema().dump(self), stream)

    @staticmethod
    def from_yaml(stream):
        """
        Load config from yaml

        Parameters
        ----------
        stream : str, file-obj
            String or file-like object to load yaml from

        Returns
        -------
        config : SessionConfig
            Generated config
        """
        return SessionConfigSchema().load(yaml.safe_load(stream=stream))


class WriterConfigSchema(Schema):
    """
    Marshmallow schema for :class:`WriterConfig` class.
    """

    type = fields.Str(validate=validate.OneOf(["local", "s3", "whylabs"]), required=True)
    formats = fields.List(
        fields.Str(validate=validate.OneOf(ALL_SUPPORTED_FORMATS)),
        required=True,
        validate=validate.Length(min=1),
    )
    output_path = fields.Str(required=True)
    path_template = fields.Str(required=False, allow_none=True)
    filename_template = fields.Str(required=False, allow_none=True)
    data_collection_consent = fields.Bool(required=False, allow_none=True)

    @post_load
    def make_writer(self, data, **kwargs):
        return WriterConfig(**data)


class SessionConfigSchema(Schema):
    """
    Marshmallow schema for :class:`SessionConfig` class.
    """

    project = fields.Str(required=True)
    pipeline = fields.Str(required=True)
    with_rotation_time = fields.Str(required=False, validate=validate.OneOf(["s", "m", "h", "d"]))
    cache = fields.Int(required=False)
    verbose = fields.Bool(missing=False)
    writers = fields.List(
        fields.Nested(WriterConfigSchema),
        validate=validate.Length(min=1),
        required=True,
    )

    @post_load
    def make_session(self, data, **kwargs):
        return SessionConfig(**data)


def load_config(path_to_config: str = None):
    """
    Load logging configuration, from disk and from the environment.

    Config is loaded by attempting to load files in the following order.  The
    first valid file will be used

    1. Path set in ``WHYLOGS_CONFIG`` environment variable
    2. Current directory's ``.whylogs.yaml`` file
    3. ``~/.whylogs.yaml`` (home directory)
    4. ``/opt/whylogs/.whylogs.yaml`` path

    Returns
    -------
    config : SessionConfig, None
        Config for the logger, if a valid config file is found, else returns
        `None`.
    """
    import os

    logger = getLogger(__name__)
    if path_to_config is None:
        cfg_candidates = {
            "enviroment": os.environ.get("WHYLOGS_CONFIG"),
            "current_dir": WHYLOGS_YML,
            "home_dir": os.path.join(os.path.expanduser("~"), WHYLOGS_YML),
            "opt": os.path.join("/opt/whylogs/", WHYLOGS_YML),
        }

        for k, f_path in cfg_candidates.items():
            logger.debug(f"Attempting to load config file: {f_path}")
            if f_path is None or not os.path.isfile(f_path):
                continue

            try:
                with open(f_path, "rt") as f:
                    session_config = SessionConfig.from_yaml(f)
                    return session_config
            except IOError as e:
                logger.warning("Failed to load YAML config", e)
    else:
        try:
            with open(path_to_config, "rt") as f:
                session_config = SessionConfig.from_yaml(f)
                return session_config
        except IOError as e:
            logger.warning("Failed to load YAML config", e)
    return None
