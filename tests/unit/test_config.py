from typing import Dict, List, Any, Union, Hashable

from whylabs.logs.config import WhyLogsConfig
import yaml
from whylabs.logs import __version__ as version_str


def test_round_trip_serialization():
    config = WhyLogsConfig("dataset-name", "datetime")
    config_yml = yaml.dump(config)
    round_trip: WhyLogsConfig = yaml.safe_load(config_yml)
    assert round_trip.dataset_name == config.dataset_name
    assert round_trip.datetime_column == config.datetime_column
    assert round_trip.version == config.version


def test_load_all_fields_present():
    yml_input = """!WhyLogsConfig
dataset_name: dataset-name
datetime_column: datetime
version: xyz"""
    config: WhyLogsConfig = yaml.safe_load(yml_input)
    assert config.dataset_name == "dataset-name"
    assert config.datetime_column == "datetime"
    assert config.version == 'xyz'


def test_load_no_version():
    yml_input = """!WhyLogsConfig
dataset_name: dataset-name
datetime_column: datetime"""
    config: WhyLogsConfig = yaml.safe_load(yml_input)
    assert config.dataset_name == "dataset-name"
    assert config.datetime_column == "datetime"
    assert config.version == version_str
