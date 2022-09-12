from datetime import date, datetime, timedelta, timezone

import pytest

from whylogs.datasets import Ecommerce
from whylogs.datasets.configs import EcommerceConfig

dataset_config = EcommerceConfig()


class TestEcommerceDataset(object):
    @pytest.fixture
    def dataset(self):
        return Ecommerce(version="base")

    def test_existing_version(self):
        dataset = Ecommerce(version="base")
        assert dataset

    def test_non_existing_version(self):
        with pytest.raises(ValueError, match="Version not found in list of available versions."):
            Ecommerce(version="nonexisting-version")

    def test_supported_interval(self, dataset):
        dataset.set_parameters("1d")
        now = datetime.now(timezone.utc) + timedelta(days=1)
        batch = dataset.get_inference_data(target_date=now)
        assert len(batch.data) > 0

    def test_non_supported_interval(self, dataset):
        with pytest.raises(
            ValueError, match="Maximum allowed interval for this dataset is {}".format(dataset_config.max_interval)
        ):
            dataset.set_parameters("50d")
        with pytest.raises(ValueError, match="Could not parse interval {}!".format("5z")):
            dataset.set_parameters("5z")

    def test_date_baseline_ts(self, dataset):
        dataset.set_parameters("1d", baseline_timestamp=datetime.now(timezone.utc).date())
        assert isinstance(dataset.baseline_timestamp, date)
        dataset.set_parameters("1d", inference_start_timestamp=date.today())
        assert isinstance(dataset.inference_start_timestamp, date)

    def test_datetime_baseline_ts(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now(), baseline_timestamp=datetime.now())
        assert isinstance(dataset.baseline_timestamp, date)
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now())
        assert isinstance(dataset.inference_start_timestamp, date)

    def test_unsupported_timestamp(self, dataset):
        with pytest.raises(ValueError, match="Could not parse string as datetime. Expected format:"):
            dataset.set_parameters("1d", baseline_timestamp="22-04-12")
        with pytest.raises(ValueError, match="Could not parse string as datetime. Expected format:"):
            dataset.set_parameters("1d", inference_start_timestamp="22-04-12")

    def test_get_baseline(self, dataset):
        dataset.set_parameters("1d", baseline_timestamp=datetime.now(timezone.utc).date())
        baseline = dataset.get_baseline()
        assert isinstance(baseline.timestamp, datetime)
        assert baseline.timestamp.tzinfo
        assert baseline.timestamp.day == datetime.now(timezone.utc).day
        assert baseline.timestamp.month == datetime.now(timezone.utc).month
        assert baseline.timestamp.year == datetime.now(timezone.utc).year
        assert len(baseline.data) > 0

    def test_get_inference_by_date(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now(timezone.utc).date())
        batch = dataset.get_inference_data(target_date=datetime.now(timezone.utc).date())
        assert batch.timestamp.day == datetime.now(timezone.utc).day
        assert batch.timestamp.month == datetime.now(timezone.utc).month
        assert batch.timestamp.year == datetime.now(timezone.utc).year
        assert len(batch.data) > 0

        batch = dataset.get_inference_data(target_date=datetime.now())
        assert batch.timestamp.day == datetime.now(timezone.utc).day
        assert batch.timestamp.month == datetime.now(timezone.utc).month
        assert batch.timestamp.year == datetime.now(timezone.utc).year
        assert len(batch.data) > 0

    def test_get_inference_pass_both_arguments(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now(timezone.utc).date())
        with pytest.raises(ValueError, match="Either date or number_batches should be passed, not both."):
            dataset.get_inference_data(target_date=datetime.now(timezone.utc).date(), number_batches=30)

    def test_get_inference_pass_no_arguments(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now(timezone.utc).date())
        with pytest.raises(ValueError, match="date or number_batches must be passed to get_inference_data."):
            dataset.get_inference_data()

    def test_get_inference_wrong_date_type(self, dataset):
        dataset.set_parameters("1d", inference_start_timestamp=datetime.now(timezone.utc).date())
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
        with pytest.raises(ValueError, match="Could not parse interval {}!".format("12y")):
            dataset.set_parameters("12y")
        with pytest.raises(
            ValueError, match="Maximum allowed interval for this dataset is {}".format(EcommerceConfig.max_interval)
        ):
            dataset.set_parameters("120d")
        with pytest.raises(
            ValueError, match="Current accepted unit for this dataset is {}".format(EcommerceConfig.base_unit)
        ):
            dataset.set_parameters("5M")

    def test_inference_df_date_matches_config_timestamp(self, dataset):
        dataset.set_parameters(original=True)
        assert dataset.inference_df.iloc[0].name == dataset_config.inference_start_timestamp["base"]

    def test_original_parameter_overrides_timestamps(self, dataset):
        dataset.set_parameters(
            baseline_timestamp=datetime.now(timezone.utc).date(),
            inference_start_timestamp=datetime.now(timezone.utc).date(),
            original=True,
        )
        assert dataset.inference_df.iloc[0].name == dataset_config.inference_start_timestamp["base"]
        assert dataset.baseline_df.iloc[0].name == dataset_config.baseline_start_timestamp["base"]
