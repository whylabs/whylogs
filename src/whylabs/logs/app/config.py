"""
"""
import abc
from abc import abstractmethod
from logging import getLogger
from typing import Optional, List

import yaml as yaml
from marshmallow import Schema, fields, post_load, validate

from whylabs.logs.app.handlers import DiskHandler


class HandlerConfig:
    """
    Simple config class
    """
    OBJECT_TYPES = ('dataset_summary', 'dataset_profile',)
    FORMATS = ('json', 'flat', 'protobuf')
    DESTINATIONS = ('s3', 'disk', 'stdout')

    def __init__(self, object_type, fmt, dest):
        assert object_type in self.OBJECT_TYPES
        assert fmt in self.FORMATS
        assert dest in self.DESTINATIONS
        self.object_type = object_type
        self.fmt = fmt
        self.dest = dest

    def __repr__(self):
        import json
        d = self.to_dict()
        try:
            msg = json.dumps(d, indent=2, sort_keys=True)
        except TypeError:
            msg = str(d)
        msg = '<HandlerConfig>\n' + msg
        return msg

    def to_dict(self):
        return {
            'object_type': self.object_type,
            'fmt': self.fmt,
            'dest': self.dest,
        }


class BaseHandlerConfig(yaml.YAMLObject):
    __metaclass__ = abc.ABCMeta

    @abstractmethod
    def get_handler(self):
        pass


class DiskHandlerConfig(BaseHandlerConfig):
    yaml_tag = '!tag:LocalDiskHandlerConfig'

    def __init__(self, fmt: str, path: str, project: str, user: str, team: Optional[str] = None):
        self.fmt = fmt
        self.folder = path
        self.project = project
        self.user = user
        self.team = team

    def get_handler(self):
        return DiskHandler(folder=self.path, team=self.team, project=self.project, user=self.user)


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
    def __init__(self, project: str, pipeline: str, verbose: bool, writers: List[WriterConfig]):
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
    type = fields.Str(validate=validate.OneOf(['local', 's3']), required=True)
    formats = fields.List(fields.Str(validate=validate.OneOf(['all', 'protobuf', 'json', 'flat'])),
                          required=True,
                          validate=validate.Length(min=1))
    output_path = fields.Str(required=True)

    @post_load
    def make_writer(self, data, **kwargs):
        return WriterConfig(**data)


class SessionConfigSchema(Schema):
    project = fields.Str(required=True)
    pipeline = fields.Str(required=True)
    verbose = fields.Bool(missing=False)
    writers = fields.List(fields.Nested(WriterConfigSchema), validate=validate.Length(min=1), required=True)

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
    user_file = os.path.join(os.path.expanduser('~'), '.whylogs.yml')
    files = [user_file, 'whylogs.yml', ]
    for fname in files:
        logger.debug(f'Attempting to load config file: {fname}')
        try:
            with open(fname, 'rt') as f:
                return SessionConfig.from_yaml(f)
        except FileNotFoundError:
            pass
    return None
