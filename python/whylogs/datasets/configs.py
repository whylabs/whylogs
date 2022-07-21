from dataclasses import dataclass, field
from typing import List


@dataclass
class WeatherConfig:
    url: str = "dummy-url"
    available_versions: tuple = ("in_domain", "out_domain")
