import datetime
import logging
import os
import tempfile

import pytest
import responses
from responses import PUT

import whylogs as why
from whylogs.api.writer import Writers
from whylogs.api.writer.whylabs import WhyLabsWriter

logger = logging.getLogger(__name__)


class TestWhylabsWriter(object):
    @classmethod
    def setup_class(cls):
        os.environ["WHYLABS_API_KEY"] = "0123456789.0any"
        os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-1"
        os.environ["WHYLABS_DEFAULT_DATASET_ID"] = "model-5"
        os.environ["WHYLABS_API_ENDPOINT"] = "https://api.whylabsapp.com"
        os.environ["WHYLABS_V1_ENABLED"] = "True"

    @classmethod
    def teardown_class(cls):
        del os.environ["WHYLABS_API_KEY"]
        del os.environ["WHYLABS_DEFAULT_ORG_ID"]
        del os.environ["WHYLABS_DEFAULT_DATASET_ID"]
        del os.environ["WHYLABS_API_ENDPOINT"]

    @pytest.fixture
    def results(self, pandas_dataframe):
        return why.log(pandas=pandas_dataframe)

    @pytest.mark.skip("Skip for now. Will need more mocking")
    def test_upload_request(self, results):
        self.responses = responses.RequestsMock()
        self.responses.start()

        self.responses.add(PUT, url="https://api.whylabsapp.com", body=results.view().to_pandas().to_json())
        profile = results.view()

        writer = WhyLabsWriter()
        # reproducing what the write method does, without explicitly calling it
        # so it's possible to inject the upload_url
        with tempfile.NamedTemporaryFile() as tmp_file:
            profile.write(path=tmp_file.name)
            tmp_file.flush()

            dataset_timestamp = profile.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)
            dataset_timestamp = int(dataset_timestamp.timestamp() * 1000)
            response = writer._do_upload(dataset_timestamp=dataset_timestamp, profile_path=tmp_file.name)
            assert response[0] is True

    @pytest.mark.skip("Skip for now. Will need more mocking")
    def test_upload_reference_request(self, results):
        self.responses = responses.RequestsMock()
        self.responses.start()

        self.responses.add(PUT, url="https://api.whylabsapp.com", body=results.view().to_pandas().to_json())
        profile = results.view()

        writer = WhyLabsWriter()
        # reproducing what the write method does, without explicitly calling it
        # so it's possible to inject the upload_url
        with tempfile.NamedTemporaryFile() as tmp_file:
            profile.write(path=tmp_file.name)
            tmp_file.flush()

            dataset_timestamp = profile.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)
            dataset_timestamp = int(dataset_timestamp.timestamp() * 1000)
            response = writer._do_upload(
                dataset_timestamp=dataset_timestamp,
                profile_path=tmp_file.name,
                reference_profile_name="RefProfileAlias",
            )
            assert response[0] is True

    @pytest.mark.skip("Skip for now. Probably need more mocking")
    def test_api_key_null_raises_error(self, results, caplog):
        caplog.set_level(logging.ERROR)
        with pytest.raises(ValueError):
            del os.environ["WHYLABS_API_KEY"]
            writer: WhyLabsWriter = Writers.get("whylabs")
            writer.write(profile=results.profile())
        os.environ["WHYLABS_API_KEY"] = "01234567890.any"

    def test_option_will_overwrite_defaults(self) -> None:
        writer = WhyLabsWriter()
        writer.option(org_id="new_org_id", dataset_id="new_dataset_id", api_key="other_api_key")
        assert writer._org_id == "new_org_id"
        assert writer._dataset_id == "new_dataset_id"
        assert writer._api_key == "other_api_key"

    def test_api_key_prefers_parameter_over_env_var(self, results, caplog):
        os.environ["WHYLABS_API_KEY"] = "0123456789.any"
        with pytest.raises(ValueError):
            results.writer("whylabs").option(org_id="org_id", api_key="api_key_123.foo").write(dataset_id="dataset_id")

    def test_writer_accepts_dest_param(self, results, caplog):
        # TODO: inspect error or mock better to avoid network call and keep test focused.
        with pytest.raises(ValueError):
            results.writer("whylabs").option(api_key="bad_key_format").write(dataset_id="dataset_id", dest="tmp")

    def test_write_response(self, results):
        with pytest.raises(ValueError):
            response = (
                results.writer("whylabs").option(api_key="bad_key_format").write(dataset_id="dataset_id", dest="tmp")
            )
            assert response[0] is True
