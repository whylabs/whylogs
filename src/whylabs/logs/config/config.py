from typing import Optional
from whylabs.logs import __version__ as version_str

import yaml

DATETIME_FORMAT_TAG = u'!WhyLogsConfig.Datetime'


def datetime_constructor(loader, node):
    fields = loader.construct_mapping(node)
    return ConfigDateTime(**fields)


yaml.add_constructor(DATETIME_FORMAT_TAG, datetime_constructor, Loader=yaml.SafeLoader)


class ConfigDateTime(yaml.YAMLObject):
    yaml_tag = DATETIME_FORMAT_TAG

    def __init__(self, column: str, fmt: Optional[str] = None):
        self.column = column
        self.fmt = fmt

    def __repr__(self):
        return f'{self.__dict__}'


def config_constructor(loader, node):
    fields = loader.construct_mapping(node)
    return WhyLogsConfig(**fields)


CONFIG_YAML_TAG = u'!WhyLogsConfig'

yaml.add_constructor(CONFIG_YAML_TAG, config_constructor, Loader=yaml.SafeLoader)


class WhyLogsConfig(yaml.YAMLObject):
    yaml_tag = CONFIG_YAML_TAG

    def __init__(self,
                 dataset_name: str,
                 datetime: Optional[ConfigDateTime] = None,
                 version: str = version_str):
        self.dataset_name = dataset_name
        self.datetime = datetime
        self.version = version

    def __repr__(self):
        return f'{self.__dict__}'
