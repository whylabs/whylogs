from whylogs.datasets import Weather
from datetime import date, datetime
import pytest

import pytest
from whylogs.datasets import Weather
import os

print(os.environ["WHYLOGS_NO_ANALYTICS"])


class TestWeatherDataset(object):
    def test_existing_version(self):
        dataset = Weather(version="in_domain")

    def test_non_existing_version(self):
        with pytest.raises(ValueError, match="Version not found in list of available versions."):
            dataset = Weather(version="nonexisting-version")

    def test_supported_interval(self):
        dataset = Weather()
        dataset.set_parameters("1d")

    def test_non_supported_interval(self):
        with pytest.raises(ValueError, match="Input interval not supported!"):
            dataset = Weather()
            dataset.set_parameters("12d")

    def test_date_baseline_ts(self):
        dataset = Weather(version="in_domain")
        dataset.set_parameters("1d", baseline_timestamp=date.today())
        assert isinstance(dataset.baseline_timestamp, date)
        dataset.set_parameters("1d", production_start_timestamp=date.today())
        assert isinstance(dataset.production_start_timestamp, date)

    def test_datetime_baseline_ts(self):
        dataset = Weather(version="in_domain")
        dataset.set_parameters("1d", baseline_timestamp=datetime.now())
        assert isinstance(dataset.baseline_timestamp, date)
        dataset.set_parameters("1d", production_start_timestamp=datetime.now())
        assert isinstance(dataset.production_start_timestamp, date)

    def test_unsupported_timestamp(self):
        dataset = Weather(version="in_domain")
        with pytest.raises(ValueError, match="You must pass either a Datetime or Date object to baseline_timestamp!"):
            dataset.set_parameters("1d", baseline_timestamp="22-04-12")
        with pytest.raises(
            ValueError, match="You must pass either a Datetime or Date object to production_start_timestamp!"
        ):
            dataset.set_parameters("1d", production_start_timestamp="22-04-12")


# TestWeatherDataset().test_non_existing_version()
# TestWeatherDataset().test_existing_version()
# TestWeatherDataset().test_non_supported_interval()
# TestWeatherDataset().test_supported_interval()
# TestWeatherDataset().test_date_baseline_ts()
# TestWeatherDataset().test_datetime_baseline_ts()
# TestWeatherDataset().test_unsupported_timestamp()


dataset = Weather(version="in_domain")
dataset.set_parameters("1d", production_start_timestamp=date.today())
# batch = dataset.get_production_data(date=date.today())
# print(batch.frame)
dataset.set_parameters("1d", baseline_timestamp=datetime.now())
baseline = dataset.get_baseline()
print(type(baseline.frame["date"][0]))
# print(baseline.timestamp)

# print(Weather.describe())

# print(dataset.baseline_timestamp)
# print(dataset.interval)
