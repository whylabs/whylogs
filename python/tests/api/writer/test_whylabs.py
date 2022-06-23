import datetime
import logging
import os
import tempfile

import pytest
import requests
import responses
from responses import HEAD, PUT
from whylabs_client.rest import ForbiddenException

import whylogs as why
from whylogs.api.writer import Writers
from whylogs.api.writer.whylabs import WhyLabsWriter

logger = logging.getLogger(__name__)


class TestWhylabsWriter(object):
    @classmethod
    def setup_class(cls):
        os.environ["WHYLABS_API_KEY"] = "0123456789.any"
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

    def test_upload_request(self, results):
        self.responses = responses.RequestsMock()
        self.responses.start()

        self.responses.add(PUT, url="https://api.whylabsapp.com", body=results.view().to_pandas().to_json())
        self.responses.add(
            HEAD,
            url="https://whylabs-public.s3.us-west-2.amazonaws.com/whylogs_config/whylabs_writer_disabled",
            headers="",
            status=200,
        )
        profile = results.view()

        writer = WhyLabsWriter()
        # reproducing what the write method does, without explicitly calling it
        # so it's possible to inject the upload_url
        with tempfile.NamedTemporaryFile() as tmp_file:
            profile.write(path=tmp_file.name)
            tmp_file.flush()

            dataset_timestamp = profile.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)
            dataset_timestamp = int(dataset_timestamp.timestamp() * 1000)
            response = writer._upload_whylabs(
                dataset_timestamp=dataset_timestamp, profile_path=tmp_file.name, upload_url="https://api.whylabsapp.com"
            )
            assert isinstance(response, requests.Response)
            assert response.status_code == 200

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

    def test_api_key_prefers_env_var(self, results, caplog):
        os.environ["WHYLABS_API_KEY"] = "0123456789.any"
        with pytest.raises(ForbiddenException):
            results.writer("whylabs").option(org_id="org_id", api_key="api_key_12.foo").write(dataset_id="dataset_id")
        assert "Failed to upload" in caplog.text
        assert "Updating API key ID" in caplog.text
