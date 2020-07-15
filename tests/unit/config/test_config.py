import yaml

from whylabs.logs import __version__ as version_str
from whylabs.logs.config import WhyLogsConfig
from whylabs.logs.config import ConfigDateTime


def test_round_trip_serialization():
    config = WhyLogsConfig("dataset-name", ConfigDateTime("datetime", "format"))
    config_yml = yaml.dump(config)
    print(config_yml)
    round_trip: WhyLogsConfig = yaml.safe_load(config_yml)
    assert round_trip.dataset_name == config.dataset_name
    assert round_trip.datetime.column == config.datetime.column
    assert round_trip.datetime.fmt == config.datetime.fmt
    assert round_trip.version == config.version


def test_load_all_fields_present():
    yml_input = """
!WhyLogsConfig
dataset_name: dataset-name
datetime: !WhyLogsConfig.Datetime
  column: datetime
  fmt: format
version: xyz
"""
    config: WhyLogsConfig = yaml.safe_load(yml_input)
    assert config.dataset_name == "dataset-name"
    assert config.datetime.column == "datetime"
    assert config.datetime.fmt == "format"
    assert config.version == 'xyz'


def test_load_no_format_no_version():
    yml_input = """
!WhyLogsConfig
dataset_name: dataset-name
datetime: !WhyLogsConfig.Datetime
  column: datetime
"""
    config: WhyLogsConfig = yaml.safe_load(yml_input)
    assert config.dataset_name == "dataset-name"
    assert config.datetime.column == "datetime"
    assert config.datetime.fmt is None
    assert config.version == version_str


def test_load_no_datetime():
    yml_input = """
!WhyLogsConfig
dataset_name: dataset-name
"""
    config: WhyLogsConfig = yaml.safe_load(yml_input)
    assert config.dataset_name == "dataset-name"
    assert config.datetime is None
    assert config.version == version_str
