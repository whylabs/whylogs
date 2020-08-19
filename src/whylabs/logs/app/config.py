"""
"""
import sys
from logging import getLogger
from typing import List

import yaml as yaml
from marshmallow import Schema, fields, post_load, validate

from whylabs.logs.app.output_formats import SUPPORTED_OUTPUT_FORMATS

WHYLOGS_YML = ".whylogs.yml"

ALL_SUPPORTED_FORMATS = ["all"] + SUPPORTED_OUTPUT_FORMATS


class WriterConfig:
    def __init__(self, type: str, formats: List[str], output_path: str):
        self.type = type
        self.formats = formats
        self.output_path = output_path

    def to_yaml(self, stream=None):
        dump = WriterConfigSchema().dump(self)
        return yaml.dump(dump, stream)

    @staticmethod
    def from_yaml(stream, **kwargs):
        data = yaml.safe_load(stream)
        return WriterConfigSchema().load(data)


class SessionConfig:
    """
    Configuration for a WhyLogs session. The 
    """

    def __init__(
        self,
        project: str,
        pipeline: str,
        writers: List[WriterConfig],
        verbose: bool = False,
    ):
        self.project = project
        self.pipeline = pipeline
        self.verbose = verbose
        self.writers = writers

    def to_yaml(self, stream=None):
        return yaml.dump(SessionConfigSchema().dump(self), stream)

    @staticmethod
    def from_yaml(stream):
        return SessionConfigSchema().load(yaml.safe_load(stream=stream))


class WriterConfigSchema(Schema):
    type = fields.Str(validate=validate.OneOf(["local", "s3"]), required=True)
    formats = fields.List(
        fields.Str(validate=validate.OneOf(ALL_SUPPORTED_FORMATS)),
        required=True,
        validate=validate.Length(min=1),
    )
    output_path = fields.Str(required=True)

    @post_load
    def make_writer(self, data, **kwargs):
        return WriterConfig(**data)


class SessionConfigSchema(Schema):
    project = fields.Str(required=True)
    pipeline = fields.Str(required=True)
    verbose = fields.Bool(missing=False)
    writers = fields.List(
        fields.Nested(WriterConfigSchema),
        validate=validate.Length(min=1),
        required=True,
    )

    @post_load
    def make_session(self, data, **kwargs):
        return SessionConfig(**data)


def load_config():
    """
    Load logging configuration, from disk and from the environment.

    Config is loaded by attempting to load files basd on:

    1. Path set in WHYLOGS_CONFIG environment variable
    2. Current directory's .whylogs.yaml file
    3. ~/.whylogs.yaml (home directory)
    4. /opt/whylogs/.whylogs.yaml path
    """
    import os

    logger = getLogger(__name__)
    cfg_candidates = [
        os.environ.get("WHYLOGS_CONFIG"),
        WHYLOGS_YML,
        os.path.join(os.path.expanduser("~"), WHYLOGS_YML),
        os.path.join("/opt/whylogs/", WHYLOGS_YML),
    ]

    for fpath in cfg_candidates:
        logger.debug(f"Attempting to load config file: {fpath}")
        if fpath is None or not os.path.isfile(fpath):
            continue

        try:
            with open(fpath, "rt") as f:
                return SessionConfig.from_yaml(f)
        except IOError as e:
            logger.warning("Failed to load YAML config", e)
            pass
    return None
