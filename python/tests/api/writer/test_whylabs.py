import datetime
import logging
import os
import random
import string
import tempfile
from unittest.mock import MagicMock, patch

import pytest
import responses
from platformdirs import PlatformDirs
from responses import PUT

import whylogs as why
from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.api.whylabs.session.config import EnvVariableName, SessionConfig
from whylogs.api.whylabs.session.session_manager import (
    SessionManager,
    get_current_session,
    init,
)
from whylogs.api.whylabs.session.session_types import SessionType
from whylogs.api.whylabs.session.whylabs_client_cache import WhylabsClientCache
from whylogs.api.writer import Writers
from whylogs.api.writer.whylabs import WhyLabsWriter
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column

logger = logging.getLogger(__name__)
dirs = PlatformDirs("whylogs_tests", "whylogs")


def _random_str(n: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


class TestWhylabsWriterWithSession(object):
    # So we don't delete our own configs while running tests
    def setup_method(self) -> None:
        WhylabsClientCache.reset()
        SessionManager.reset()

        os.environ[EnvVariableName.WHYLOGS_CONFIG_PATH.value] = f"{dirs.user_cache_dir}/whylogs_{_random_str(5)}.ini"

        config = SessionConfig()
        config.reset_config()

    def teardown_method(self) -> None:
        WhylabsClientCache.reset()
        SessionManager.reset()
        del os.environ[EnvVariableName.WHYLOGS_CONFIG_PATH.value]

    @pytest.fixture
    def results(self, pandas_dataframe):
        return why.log(pandas=pandas_dataframe)

    def test_writer_throws_for_anon_sessions(self, results) -> None:
        old_key = os.environ.get(EnvVariableName.WHYLABS_API_KEY.value, None)
        if EnvVariableName.WHYLABS_API_KEY.value in os.environ:
            os.environ.pop(EnvVariableName.WHYLABS_API_KEY.value)
        session = init()  # Default session is anonymous in this case (no config file content)
        if old_key:
            os.environ[EnvVariableName.WHYLABS_API_KEY.value] = old_key

        assert session.get_type() == SessionType.WHYLABS_ANONYMOUS

        with pytest.raises(ValueError):
            WhyLabsWriter().write(results.profile())

    def test_writer_works_for_anon_with_overrides(self) -> None:
        old_key = os.environ.get(EnvVariableName.WHYLABS_API_KEY.value, None)
        if EnvVariableName.WHYLABS_API_KEY.value in os.environ:
            os.environ.pop(EnvVariableName.WHYLABS_API_KEY.value)
        key_id = "MPq7Hg002z"
        org_id = "org-xxxxxx"
        api_key = f"{key_id}.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:{org_id}"

        # You can use the writer if the session is anonymous, but you must provide the required args
        session = init()  # Default session is anonymous in this case (no config file content)

        if old_key:
            os.environ[EnvVariableName.WHYLABS_API_KEY.value] = old_key

        assert session.get_type() == SessionType.WHYLABS_ANONYMOUS

        # No error
        writer = WhyLabsWriter(
            api_key=api_key,
            dataset_id="dataset_id",
            org_id="org_id",
        )

        assert writer.key_id == api_key.split(".")[0]

    def test_writer_uses_session_for_creds(self) -> None:
        key_id = "MPq7Hg002z"
        org_id = "org-xxxxxx"
        dataset_id = "model-2"
        api_key = f"{key_id}.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:{org_id}"
        session = init(whylabs_api_key=api_key, default_dataset_id=dataset_id)

        assert session.get_type() == SessionType.WHYLABS

        writer = WhyLabsWriter()
        assert writer.key_id == key_id
        assert writer._org_id == org_id
        assert writer._dataset_id == dataset_id

    def test_writer_uses_session_for_creds_implicitly(self) -> None:
        key_id = "MPq7Hg002z"
        org_id = "org-xxxxxx"
        dataset_id = "model-2"
        api_key = f"{key_id}.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:{org_id}"
        os.environ["WHYLABS_API_KEY"] = api_key
        os.environ["WHYLABS_DEFAULT_DATASET_ID"] = dataset_id

        writer = WhyLabsWriter()
        assert writer.key_id == key_id
        assert writer._org_id == org_id
        assert writer._dataset_id == dataset_id

        session = get_current_session()
        assert session is not None
        # Session is local, so nothing happens automatically outside of writer.
        assert session.get_type() == SessionType.LOCAL

        del os.environ["WHYLABS_API_KEY"]
        del os.environ["WHYLABS_DEFAULT_DATASET_ID"]

    def test_implicit_session_init_fails_without_env_config_set(self, results) -> None:
        with pytest.raises(ValueError):
            WhyLabsWriter().write(results)

    def test_implicit_init_works_from_config_file(self) -> None:
        # Generate a len 10 alphanum string
        key_id = _random_str(10)
        org_id = f"org-{_random_str(6)}"
        dataset_id = "model-2"
        api_key = f"{key_id}.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:{org_id}"

        #
        # Part 1: Set up a config file
        #
        config = SessionConfig()
        config.reset_config()
        config.set_api_key(api_key)
        config.set_default_dataset_id(dataset_id)
        session = get_current_session()
        assert session is None  # No init happened yet

        #
        # Part 2: Make sure the implicit init uses the config file
        #
        writer = WhyLabsWriter()
        assert writer.key_id == key_id
        assert writer._org_id == org_id
        assert writer._dataset_id == dataset_id

        session = get_current_session()
        assert session is not None
        assert session.get_type() == SessionType.LOCAL

        # Clean it up
        config.reset_config()


_api_key = "0123456789.any"


class TestWhylabsWriter(object):
    @classmethod
    def setup_class(cls) -> None:
        os.environ["WHYLABS_API_KEY"] = _api_key
        os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-1"
        os.environ["WHYLABS_DEFAULT_DATASET_ID"] = "model-5"
        os.environ["WHYLABS_API_ENDPOINT"] = "https://api.whylabsapp.com"
        os.environ["WHYLABS_V1_ENABLED"] = "True"
        os.environ[EnvVariableName.WHYLOGS_CONFIG_PATH.value] = f"/tmp/test_why_{_random_str(5)}.ini"

    @classmethod
    def teardown_class(cls) -> None:
        del os.environ["WHYLABS_API_KEY"]
        del os.environ["WHYLABS_DEFAULT_ORG_ID"]
        del os.environ["WHYLABS_DEFAULT_DATASET_ID"]
        del os.environ["WHYLABS_API_ENDPOINT"]
        del os.environ["WHYLABS_V1_ENABLED"]
        del os.environ[EnvVariableName.WHYLOGS_CONFIG_PATH.value]

    def setup_method(self) -> None:
        init()

    def teardown_method(self) -> None:
        WhylabsClientCache.reset()
        SessionManager.reset()

    @pytest.fixture
    def results(self, pandas_dataframe):
        return why.log(pandas=pandas_dataframe)

    @pytest.fixture
    def segmented_result(self, pandas_dataframe):
        segment_column = "animal"
        segmented_schema = DatasetSchema(segments=segment_on_column(segment_column))
        return why.log(pandas=pandas_dataframe, schema=segmented_schema)

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

    @pytest.mark.skip("Skip for now. Will need less mocking")
    def test_upload_segmented_reference_request(self, segmented_result):
        writer = WhyLabsWriter().option(reference_profile_name="RefProfileAlias")
        writer.write = MagicMock(return_value=(True, "RefProfileAlias"))
        result = writer.write(segmented_result)

        writer.write.assert_called_with(segmented_result)
        assert isinstance(segmented_result, SegmentedResultSet)
        assert result == (True, "RefProfileAlias")

    @pytest.mark.skip("Skip for now. Will need less mocking")
    def test_segmented_result_writer(self, segmented_result):
        segmented_result_writer = segmented_result.writer("whylabs").option(reference_profile_name="RefProfileAlias")
        segmented_result_writer.write = MagicMock(return_value=(True, "RefProfileAlias"))
        result = segmented_result_writer.write()
        assert isinstance(segmented_result, SegmentedResultSet)
        assert result == (True, "RefProfileAlias")

    @pytest.mark.skip("Skip for now. Probably need more mocking")
    def test_api_key_null_raises_error(self, results, caplog):
        caplog.set_level(logging.ERROR)
        with pytest.raises(ValueError):
            del os.environ["WHYLABS_API_KEY"]
            writer: WhyLabsWriter = Writers.get("whylabs")
            writer.write(file=results.profile())
        os.environ["WHYLABS_API_KEY"] = "01234567890.any"

    @pytest.mark.skip("Skip for now. Will need less mocking")
    def test_put_feature_weight(self):
        weights = {
            "col1": 0.7,
            "col2": 0.3,
            "col3": 0.01,
        }

        feature_weights = FeatureWeights(weights)
        writer = WhyLabsWriter()
        writer.write = MagicMock(return_value=(True, "200"))
        result = writer.write(feature_weights)

        writer.write.assert_called_with(feature_weights)
        assert result == (True, "200")

    @pytest.mark.skip("Skip for now. Will need less mocking")
    def test_put_feature_weight_writer(self):
        weights = {
            "col1": 0.7,
            "col2": 0.3,
            "col3": 0.01,
        }

        feature_weights = FeatureWeights(weights)
        feature_weights_writer = feature_weights.writer("whylabs")
        feature_weights_writer.write = MagicMock(return_value=(True, "200"))
        result = feature_weights_writer.write()
        assert result == (True, "200")

    @pytest.mark.skip("Skip for now. Will need less mocking")
    def test_get_feature_weight(self):
        writer = WhyLabsWriter()
        get_result = FeatureWeights(
            weights={"col1": 0.7, "col2": 0.3, "col3": 0.01},
            metadata={"version": 13, "updatedTimestamp": 1663620626701, "author": "system"},
        )

        writer.get_feature_weights = MagicMock(return_value=get_result)
        result = writer.get_feature_weights()
        assert result == get_result
        assert isinstance(result, FeatureWeights)

    @pytest.mark.skip("Skip for now. Will need less mocking")
    def test_flag_column_as_custom_performance_metric(self):
        writer = WhyLabsWriter()
        column_name = "col_name"
        label = "customMetric"
        default_metric = "mean"
        flag_result = (True, "{'request_id': '0dfe61f9-36c4-46b0-b176-a62f4f3c85e0'}")
        writer.tag_custom_performance_column = MagicMock(return_value=flag_result)
        result = writer.tag_custom_performance_column(column=column_name, label=label, default_metric=default_metric)
        assert result == flag_result

    def test_option_will_overwrite_defaults(self) -> None:
        writer = WhyLabsWriter()
        writer.option(
            org_id="new_org_id",
            dataset_id="new_dataset_id",
            api_key="newkeynewk.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        assert writer._org_id == "new_org_id"
        assert writer._dataset_id == "new_dataset_id"
        assert writer.key_id == "newkeynewk"

    def test_api_key_prefers_parameter_over_env_var(self, results, caplog):
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

    def test_changing_api_key_works(self) -> None:
        #
        # Defaults
        #
        writer = WhyLabsWriter()  # Using test level default api key via the session
        assert writer.key_id == _api_key.split(".")[0]
        cache_api_key_ids = [it.api_key for it in WhylabsClientCache.instance()._api_client_cache.keys()]
        assert cache_api_key_ids == [_api_key]

        #
        # Change 1
        #
        key2 = "2222222222.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        writer.option(api_key=key2)
        assert writer.key_id == key2.split(".")[0]
        cache_api_key_ids = [it.api_key for it in WhylabsClientCache.instance()._api_client_cache.keys()]
        assert cache_api_key_ids == [_api_key, key2]

        #
        # Change to original
        #
        writer.option(api_key=_api_key)
        assert writer.key_id == _api_key.split(".")[0]
        cache_api_key_ids = [it.api_key for it in WhylabsClientCache.instance()._api_client_cache.keys()]
        assert cache_api_key_ids == [_api_key, key2]

    def test_custom_client(self) -> None:
        client = MagicMock()
        writer = WhyLabsWriter(api_client=client)

        # It really can't make any sense. It still uses an EnvKeyRefresher when someone gives it an custom client so
        # the key won't actually match w/e the client is using, but this is the way it currently behaves. It can't
        # make sense until we refactor how the client is used.
        with pytest.raises(AttributeError):
            # Fails because the key refresher wouldn't have been called yet presumably, and the key id won't be
            # cached, but even if it was it wouldn't be the right key id because it was never set on the client.
            writer.key_id

        assert len(WhylabsClientCache.instance()._api_client_cache) == 0

    @patch(
        "whylogs.api.writer.whylabs.WhyLabsWriter._get_upload_url_segmented_reference",
        return_value=("profile1", "http://upload.url"),
    )
    @patch("whylogs.api.writer.whylabs.WhyLabsWriter._do_upload", return_value=(True, "Success"))
    @patch("whylogs.api.writer.whylabs.WhyLabsWriter._get_dataset_timestamp", return_value=1234567890)
    @patch("whylogs.api.writer.whylabs.WhyLabsWriter._upload_zipped_files", return_value=(True, "Success"))
    def test_write_segmented_reference_result_set(
        self, mock_do_upload, mock_get_url, mock_get_dataset_timestamp, mock_upload_zipped_files
    ):
        mock_file = MagicMock()
        # Mock view.write() method
        writable_mock = MagicMock()
        mock_file.get_writables.return_value = [writable_mock]

        writer = WhyLabsWriter()
        result = writer._write_segmented_reference_result_set(file=mock_file, zip_file=True)
        assert result == (True, "Success")

        writer._get_dataset_timestamp.assert_called_once()
        writer._get_upload_url_segmented_reference.assert_called_once_with([], 1234567890, zip_file=True)
        writer._upload_zipped_files.assert_called_once_with(
            files=[writable_mock],
            dataset_timestamp_epoch=1234567890,
            upload_url="http://upload.url",
            profile_id="profile1",
        )
        writer._do_upload.assert_not_called()

        result_not_zipped = WhyLabsWriter()._write_segmented_reference_result_set(file=mock_file, zip_file=False)
        assert result_not_zipped == (True, "Success")
