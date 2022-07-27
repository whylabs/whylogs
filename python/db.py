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
        dataset.set_parameters("1d", inference_start_timestamp=date.today())
        assert isinstance(dataset.inference_start_timestamp, date)

    def test_datetime_baseline_ts(self):
        dataset = Weather(version="in_domain")
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now(), baseline_timestamp=datetime.now())
        assert isinstance(dataset.baseline_timestamp, date)
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now())
        assert isinstance(dataset.inference_start_timestamp, date)

    def test_unsupported_timestamp(self):
        dataset = Weather(version="in_domain")
        with pytest.raises(ValueError, match="You must pass either a Datetime or Date object to timestamp!"):
            dataset.set_parameters("1d", baseline_timestamp="22-04-12")
        with pytest.raises(ValueError, match="You must pass either a Datetime or Date object to timestamp!"):
            dataset.set_parameters("1d", inference_start_timestamp="22-04-12")

    def test_get_baseline(self):
        dataset = Weather(version="in_domain")
        dataset.set_parameters("1d", baseline_timestamp=date.today())
        baseline = dataset.get_baseline()
        assert baseline.timestamp == date.today()
        assert len(baseline.frame) > 0

    def test_get_inference_by_date(self):
        dataset = Weather(version="in_domain")
        dataset.set_parameters("1d", inference_start_timestamp=date.today())

        batch = dataset.get_inference_data(target_date=date.today())
        assert batch.timestamp == date.today()
        assert len(batch.frame) > 0

        batch = dataset.get_inference_data(target_date=datetime.now())
        assert batch.timestamp == date.today()
        assert len(batch.frame) > 0

    def test_get_inference_pass_both_arguments(self):
        dataset = Weather(version="in_domain")
        dataset.set_parameters("1d", inference_start_timestamp=date.today())
        with pytest.raises(ValueError, match="Either date or number_batches should be passed, not both."):
            batch = dataset.get_inference_data(target_date=date.today(), number_batches=30)

    def test_get_inference_pass_no_arguments(self):
        dataset = Weather(version="in_domain")
        dataset.set_parameters("1d", inference_start_timestamp=date.today())
        with pytest.raises(ValueError, match="date or number_batches must be passed to get_inference_data."):
            batch = dataset.get_inference_data()

    def test_get_inference_wrong_date_type(self):
        dataset = Weather(version="in_domain")
        dataset.set_parameters("1d", inference_start_timestamp=date.today())
        with pytest.raises(ValueError, match="Target date should be either of date or datetime type."):
            batch = dataset.get_inference_data(target_date="2022-02-12")

    def test_df_date_matches_config_timestamp(self):  # todo
        pass


# TestWeatherDataset().test_non_existing_version()
# TestWeatherDataset().test_existing_version()
# TestWeatherDataset().test_non_supported_interval()
# TestWeatherDataset().test_supported_interval()
# TestWeatherDataset().test_date_baseline_ts()
TestWeatherDataset().test_datetime_baseline_ts()
# TestWeatherDataset().test_unsupported_timestamp()
# TestWeatherDataset().test_get_baseline()
# TestWeatherDataset().test_get_inference_by_date()
# TestWeatherDataset().test_get_inference_pass_both_arguments()
# TestWeatherDataset().test_get_inference_pass_no_arguments()
# TestWeatherDataset().test_get_inference_wrong_date_type()

# dataset = Weather(version="in_domain")
# dataset.set_parameters("1d", inference_start_timestamp=date.today())
# batch = dataset.get_inference_data(target_date=date.today())
# print(batch.timestamp)
# print(len(batch.frame))
# batch = dataset.get_inference_data(target_date=datetime.now())
# print(batch.timestamp)
# print(len(batch.frame))
