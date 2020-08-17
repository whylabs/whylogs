"""
"""
from logging import getLogger
from typing import List

import yaml as yaml
from marshmallow import Schema, fields, post_load, validate


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
    def __init__(
        self, project: str, pipeline: str, verbose: bool, writers: List[WriterConfig]
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
        fields.Str(validate=validate.OneOf(["all", "protobuf", "json", "flat"])),
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

    Config is loaded in steps as follows, with later steps taking precedence:

    1. ~/.whylogs.yaml
    2. ./whylogs.yaml   (within current directory)
    3. Environment
    """
    import os

    logger = getLogger(__name__)
    user_file = os.path.join(os.path.expanduser("~"), ".whylogs.yml")
    files = [
        user_file,
        "whylogs.yml",
    ]
    for fname in files:
        logger.debug(f"Attempting to load config file: {fname}")
        try:
            with open(fname, "rt") as f:
                return SessionConfig.from_yaml(f)
        except FileNotFoundError:
            pass
    return None
