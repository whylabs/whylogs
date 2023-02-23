from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class BaseConfig:
    data_folder: str = "whylogs_data"
    description_folder: str = "whylogs.datasets.descr"


@dataclass
class DatasetConfig:
    folder_name: str
    url: str
    baseline_start_timestamp: Dict[str, datetime]
    inference_start_timestamp: Optional[Dict[str, datetime]]
    production_start_timestamp: Optional[Dict[str, datetime]]
    max_interval: int
    base_unit: str
    available_versions: tuple
    target_columns: dict
    prediction_columns: dict
    miscellaneous_columns: dict
    ignore_columns: dict


@dataclass
class EcommerceConfig(DatasetConfig):
    folder_name: str = "ecommerce"
    description_file: str = "ecommerce.rst"
    available_versions: tuple = ("base",)
    # url: str = "./whylogs/datasets"
    url: str = "https://whylabs-public.s3.us-west-2.amazonaws.com/whylogs_examples/Ecommerce"
    baseline_start_timestamp: Dict[str, datetime] = field(
        default_factory=lambda: {
            "base": datetime(year=2022, month=8, day=9, tzinfo=timezone.utc),
        }
    )
    inference_start_timestamp: Dict[str, datetime] = field(
        default_factory=lambda: {
            "base": datetime(year=2022, month=8, day=19, tzinfo=timezone.utc),
        }
    )
    production_start_timestamp: Dict[str, datetime] = field(
        default_factory=lambda: {
            "base": datetime(year=2022, month=8, day=19, tzinfo=timezone.utc),
        }
    )
    max_interval: int = 21
    base_unit: str = "D"
    target_columns: Dict[str, List[str]] = field(default_factory=lambda: {"base": ["output_discount"]})
    prediction_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "base": ["output_prediction", "output_score"],
        }
    )
    miscellaneous_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "base": [
                "category.Baby Care",
                "category.Bakery, Cakes and Dairy",
                "category.Beauty and Hygiene",
                "category.Beverages",
                "category.Cleaning and Household",
                "category.Eggs, Meat and Fish",
                "category.Foodgrains, Oil and Masala",
                "category.Fruits and Vegetables",
                "category.Gourmet and World Food",
                "category.Kitchen, Garden and Pets",
                "category.Snacks and Branded Foods",
            ],
        }
    )
    ignore_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "base": ["date"],
        }
    )


@dataclass
class EmployeeConfig(DatasetConfig):
    folder_name: str = "employee"
    description_file: str = "employee.rst"
    available_versions: tuple = ("base",)
    url: str = "https://whylabs-public.s3.us-west-2.amazonaws.com/whylogs_examples/Employee"
    baseline_start_timestamp: Dict[str, datetime] = field(
        default_factory=lambda: {
            "base": datetime(year=2023, month=1, day=16, tzinfo=timezone.utc),
        }
    )
    inference_start_timestamp: Dict[str, datetime] = field(
        default_factory=lambda: {
            "base": datetime(year=2023, month=1, day=17, tzinfo=timezone.utc),
        }
    )
    production_start_timestamp: Dict[str, datetime] = field(
        default_factory=lambda: {
            "base": datetime(year=2023, month=1, day=17, tzinfo=timezone.utc),
        }
    )
    max_interval: int = 31
    base_unit: str = "D"
    target_columns: Dict[str, List[str]] = field(default_factory=lambda: {"base": []})
    prediction_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "base": [],
        }
    )
    miscellaneous_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "base": ["assignment_category"],
        }
    )
    ignore_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "base": ["date"],
        }
    )


@dataclass
class WeatherConfig(DatasetConfig):
    folder_name: str = "weather_forecast"
    description_file: str = "weather.rst"
    url: str = "https://whylabs-public.s3.us-west-2.amazonaws.com/whylogs_examples/WeatherForecast"
    baseline_start_timestamp: Dict[str, datetime] = field(
        default_factory=lambda: {
            "in_domain": datetime(year=2018, month=9, day=1, tzinfo=timezone.utc),
            "out_domain": datetime(year=2018, month=9, day=1, tzinfo=timezone.utc),
        }
    )
    inference_start_timestamp: Dict[str, datetime] = field(
        default_factory=lambda: {
            "in_domain": datetime(year=2019, month=2, day=1, tzinfo=timezone.utc),
            "out_domain": datetime(year=2019, month=5, day=14, tzinfo=timezone.utc),
        }
    )
    production_start_timestamp: Dict[str, datetime] = field(
        default_factory=lambda: {
            "in_domain": datetime(year=2019, month=2, day=1, tzinfo=timezone.utc),
            "out_domain": datetime(year=2019, month=5, day=14, tzinfo=timezone.utc),
        }
    )
    production_start_timestamp = None
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
            "in_domain": ["meta_latitude", "meta_longitude", "meta_climate"],
            "out_domain": ["meta_latitude", "meta_longitude", "meta_climate"],
        }
    )
    ignore_columns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "in_domain": ["date"],
            "out_domain": ["date"],
        }
    )
