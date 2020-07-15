from typing import Optional
from whylabs.logs import __version__ as version_str

import yaml


def config_constructor(loader, node):
    fields = loader.construct_mapping(node)
    return WhyLogsConfig(**fields)


YAML_TAG = u'!WhyLogsConfig'

yaml.add_constructor(YAML_TAG, config_constructor, Loader=yaml.SafeLoader)


class WhyLogsConfig(yaml.YAMLObject):
    yaml_tag = YAML_TAG

    def __init__(self,
                 dataset_name: str,
                 datetime_column: Optional[str],
                 datetime_format: Optional[str] = None,
                 version: str = version_str):
        self.dataset_name = dataset_name
        self.datetime_column = datetime_column
        self.datetime_format = datetime_format
        self.version = version

    def __repr__(self):
        return f'dataset_name: {self.dataset_name}; datetime_column: {self.datetime_column}; version: {self.version}'
