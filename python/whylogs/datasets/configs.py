from dataclasses import dataclass, field
from typing import List
from datetime import date


@dataclass
class WeatherConfig:
    url: str = "https://whylabs-public.s3.us-west-2.amazonaws.com/whylogs_examples/WeatherForecast"
    baseline_start_timestamp = date(2018, 9, 1)
    production_start_timestamp = date(2019, 2, 1)
    available_versions: tuple = ("in_domain", "out_domain")
