import os
from abc import ABC, abstractmethod
from typing import List

from whylabs.logs.app.config import WriterConfig
from whylabs.logs.core import DatasetProfile


class Writer(ABC):
    def __init__(self, output_path: str, formats: List[str]):
        self.path = output_path
        self.formats = formats

    @abstractmethod
    def write(self, profile: DatasetProfile):
        pass


class LocalWriter(Writer):
    def __init__(self, output_path: str, formats: List[str]):
        if not os.path.exists(output_path):
            raise FileNotFoundError('')
        super().__init__(output_path, formats)

    def write(self, profile: DatasetProfile):
        pass


class S3Writer(Writer):
    def __init__(self, output_path: str, formats: List[str]):
        if not os.path.exists(output_path):
            raise FileNotFoundError('')
        super().__init__(output_path, formats)

    def write(self, profile: DatasetProfile):
        pass


def writer_from_config(config: WriterConfig):
    if config.type == 'local':
        return LocalWriter(config.output_path, config.formats)
    elif config.type == 's3':
        return S3Writer(config.output_path, config.formats)
    else:
        raise ValueError(f'Unknown writer type: {config.type}')
