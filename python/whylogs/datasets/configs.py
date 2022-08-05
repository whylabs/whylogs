from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List


@dataclass
class BaseConfig:
    data_folder: str = "whylogs_data"
    description_folder: str = "whylogs.datasets.descr"


@dataclass
class DatasetConfig:
    folder_name: str
    url: str
    baseline_start_timestamp: Dict[str, date]
    inference_start_timestamp: Dict[str, date]
    max_interval: int
    base_unit: str
    available_versions: tuple
    target_columns: dict
    prediction_columns: dict
    miscellaneous_columns: dict


@dataclass
class WeatherConfig(DatasetConfig):
    folder_name: str = "weather_forecast"
    description_file: str = "weather.rst"
    url: str = "https://whylabs-public.s3.us-west-2.amazonaws.com/whylogs_examples/WeatherForecast"
    baseline_start_timestamp: Dict[str, date] = field(
        default_factory=lambda: {
            "in_domain": date(2018, 9, 1),
            "out_domain": date(2018, 9, 1),
        }
    )
    inference_start_timestamp: Dict[str, date] = field(
        default_factory=lambda: {
            "in_domain": date(2019, 2, 1),
            "out_domain": date(2019, 5, 14),
        }
    )

    max_interval: int = 30
    base_unit: str = "D"
    available_versions: tuple = ("in_domain", "out_domain")
    target_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {"in_domain": ["temperature"], "out_domain": ["temperature"]}
    )
    prediction_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "in_domain": ["prediction_temperature", "uncertainty"],
            "out_domain": ["prediction_temperature", "uncertainty"],
        }
    )
    miscellaneous_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "in_domain": ["meta_latitude", "meta_longitude", "meta_climate", "date"],
            "out_domain": ["meta_latitude", "meta_longitude", "meta_climate", "date"],
        }
    )
