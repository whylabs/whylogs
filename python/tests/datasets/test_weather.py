from datetime import date, datetime

import pytest

from whylogs.datasets import Weather
from whylogs.datasets.configs import WeatherConfig

dataset_config = WeatherConfig()


class TestWeatherDataset(object):
    @pytest.fixture
    def dataset(self):
        return Weather(version="in_domain")

    def test_existing_version(self):
        dataset = Weather(version="in_domain")
        assert dataset

    def test_non_existing_version(self):
        with pytest.raises(ValueError, match="Version not found in list of available versions."):
            Weather(version="nonexisting-version")

    def test_supported_interval(self, dataset):
        dataset.set_parameters("1d")

    def test_non_supported_interval(self, dataset):
        with pytest.raises(
            ValueError, match="Maximum allowed interval for this dataset is {}".format(dataset_config.max_interval)
        ):
            dataset.set_parameters("50d")
        with pytest.raises(ValueError, match="Could not parse interval!"):
            dataset.set_parameters("5z")

    def test_date_baseline_ts(self, dataset):
        dataset.set_parameters("1d", baseline_timestamp=date.today())
        assert isinstance(dataset.baseline_timestamp, date)
        dataset.set_parameters("1d", inference_start_timestamp=date.today())
        assert isinstance(dataset.inference_start_timestamp, date)

    def test_datetime_baseline_ts(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now(), baseline_timestamp=datetime.now())
        assert isinstance(dataset.baseline_timestamp, date)
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now())
        assert isinstance(dataset.inference_start_timestamp, date)

    def test_unsupported_timestamp(self, dataset):
        with pytest.raises(ValueError, match="You must pass either a Datetime or Date object to timestamp!"):
            dataset.set_parameters("1d", baseline_timestamp="22-04-12")
        with pytest.raises(ValueError, match="You must pass either a Datetime or Date object to timestamp!"):
            dataset.set_parameters("1d", inference_start_timestamp="22-04-12")

    def test_get_baseline(self, dataset):
        dataset.set_parameters("1d", baseline_timestamp=date.today())
        baseline = dataset.get_baseline()
        assert baseline.timestamp == date.today()
        assert len(baseline.data) > 0

    def test_get_inference_by_date(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=date.today())

        batch = dataset.get_inference_data(target_date=date.today())
        assert batch.timestamp == date.today()
        assert len(batch.data) > 0

        batch = dataset.get_inference_data(target_date=datetime.now())
        assert batch.timestamp == date.today()
        assert len(batch.data) > 0

    def test_get_inference_pass_both_arguments(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=date.today())
        with pytest.raises(ValueError, match="Either date or number_batches should be passed, not both."):
            dataset.get_inference_data(target_date=date.today(), number_batches=30)

    def test_get_inference_pass_no_arguments(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=date.today())
        with pytest.raises(ValueError, match="date or number_batches must be passed to get_inference_data."):
            dataset.get_inference_data()

    def test_get_inference_wrong_date_type(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=date.today())
        with pytest.raises(ValueError, match="Target date should be either of date or datetime type."):
            dataset.get_inference_data(target_date="2022-02-12")

    def test_get_inference_without_set_parameters(self, dataset):
        batches = dataset.get_inference_data(number_batches=3)
        assert len(list(batches)) == 3

    def test_get_inference_valid_interval(self, dataset):
        dataset.set_parameters("3d")
        batches = dataset.get_inference_data(number_batches=3)
        assert len(list(batches)) == 3

    def test_get_inference_invalid_interval(self, dataset):
        with pytest.raises(ValueError, match="Could not parse interval!"):
            dataset.set_parameters("12y")
        with pytest.raises(
            ValueError, match="Maximum allowed interval for this dataset is {}".format(WeatherConfig.max_interval)
        ):
            dataset.set_parameters("120d")
        with pytest.raises(
            ValueError, match="Current accepted unit for this dataset is {}".format(WeatherConfig.base_unit)
        ):
            dataset.set_parameters("5M")

    def test_inference_df_date_matches_config_timestamp(self, dataset):
        dataset.set_parameters(original=True)
        assert dataset.inference_df.iloc[0].name == WeatherConfig.inference_start_timestamp

    def test_original_parameter_overrides_timestamps(self, dataset):
        dataset.set_parameters(baseline_timestamp=date.today(), inference_start_timestamp=date.today(), original=True)
        assert dataset.inference_df.iloc[0].name == WeatherConfig.inference_start_timestamp
        assert dataset.baseline_df.iloc[0].name == WeatherConfig.baseline_start_timestamp
