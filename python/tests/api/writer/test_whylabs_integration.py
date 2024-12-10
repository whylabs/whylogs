import logging
import os
import sys
import time
from uuid import uuid4

if sys.version_info < (3, 10):
    import httpretty
else:

    class httpretty:
        @staticmethod
        def activate(**kwargs):
            return lambda x: x


import numpy as np
import pandas as pd
import pytest
from whylabs_client.api.dataset_profile_api import DatasetProfileApi
from whylabs_client.api.models_api import ModelsApi
from whylabs_client.model.column_schema import ColumnSchema
from whylabs_client.model.entity_schema import EntitySchema
from whylabs_client.model.profile_traces_response import ProfileTracesResponse
from whylabs_client.model.reference_profile_item_response import (
    ReferenceProfileItemResponse,
)
from whylabs_client.rest import ApiException

import whylogs as why
import whylogs.api.writer.whylabs_client as wlc
from whylogs.api.writer.whylabs import WhyLabsTransaction, WhyLabsWriter
from whylogs.api.writer.whylabs_client import TransactionAbortedException, WhyLabsClient
from whylogs.api.writer.whylabs_transaction_writer import WhyLabsTransactionWriter
from whylogs.core import DatasetProfileView
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.experimental.performance_estimation.estimation_results import (
    EstimationResult,
)
from whylogs.migration.uncompound import _uncompound_performance_estimation_magic_string

# TODO: These won't work well if multiple tests run concurrently

os.environ["WHYLOGS_NO_ANALYTICS"] = "True"
# WHYLABS_API_ENDPOINT, WHYLABS_API_KEY, WHYLABS_DEFAULT_ORG_ID, and
# WHYLABS_DEFAULT_DATASET_ID need to come from the environment


SLEEP_TIME = 30

logger = logging.getLogger(__name__)


# It _appears_ that HTTPretty tests have to execute before Non-HTTPretty
# tests or else the fakes don't work.

# speed up testing
wlc.MAX_REQUEST_TIME = 2
wlc.MAX_REQUEST_TRIES = 3


def _get_org() -> str:
    key = os.environ["WHYLABS_API_KEY"]
    return key[-5:]


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_whylabs_writer_throttle_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    MODEL_ID = "XXX"
    uri = f"{ENDPOINT}/v1/log/async"
    httpretty.register_uri(httpretty.POST, uri, status=429)  # Fake WhyLabs that throttles
    why.init(reinit=True, force_local=True)
    data = {"col1": 1, "col2": "foo"}
    result = why.log(data)
    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    with pytest.raises(ApiException) as exc_info:
        writer.write(result.profile())
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_performance_column_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    ORG_ID = _get_org()
    MODEL_ID = "XXX"
    uri = f"{ENDPOINT}/v0/organizations/{ORG_ID}/models/{MODEL_ID}/schema/metric"
    httpretty.register_uri(httpretty.PUT, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    status, _ = writer.tag_custom_performance_column("col1", "perf_column", "mean")
    assert not status


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_get_transaction_id_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    uri = f"{ENDPOINT}/v1/transaction"
    httpretty.register_uri(httpretty.POST, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter()
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client.get_transaction_id()
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_set_column_schema_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    ORG_ID = _get_org()
    MODEL_ID = "XXX"
    COL_NAME = "foo"
    uri = f"{ENDPOINT}/v0/organizations/{ORG_ID}/models/{MODEL_ID}/schema/column/{COL_NAME}"
    httpretty.register_uri(httpretty.PUT, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client._set_column_schema(COL_NAME, ColumnSchema("", "", ""))
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_commit_transaction_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    uri = f"{ENDPOINT}/v1/transaction/commit"
    httpretty.register_uri(httpretty.POST, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter()
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client.commit_transaction("xxx")
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_abort_transaction_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    uri = f"{ENDPOINT}/v1/transaction/abort"
    httpretty.register_uri(httpretty.POST, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter()
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client.abort_transaction("xxx")
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_transaction_status_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    uri = f"{ENDPOINT}/v1/transaction"
    httpretty.register_uri(httpretty.GET, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter()
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client.transaction_status("xxx")
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_log_transaction_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    uri = f"{ENDPOINT}/v1/transaction/log"
    httpretty.register_uri(httpretty.POST, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter()
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client.get_upload_url_transaction(0)
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_get_feature_weights_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    ORG_ID = _get_org()
    MODEL_ID = "xxx"
    uri = f"{ENDPOINT}/v0/organizations/{ORG_ID}/dataset/{MODEL_ID}/weights"
    httpretty.register_uri(httpretty.GET, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client.get_feature_weights()
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_write_feature_weights_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    ORG_ID = _get_org()
    MODEL_ID = "xxx"
    uri = f"{ENDPOINT}/v0/organizations/{ORG_ID}/dataset/{MODEL_ID}/weights"
    httpretty.register_uri(httpretty.PUT, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    with pytest.raises(ApiException) as exc_info:
        weights = FeatureWeights({"col1": 1.0, "col2": 0.0})
        _ = writer._whylabs_client.write_feature_weights(weights)
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_get_column_schema_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    ORG_ID = _get_org()
    MODEL_ID = "xxx"
    COLUMN_NAME = "foo"
    uri = f"{ENDPOINT}/v0/organizations/{ORG_ID}/models/{MODEL_ID}/schema/column/{COLUMN_NAME}"
    httpretty.register_uri(httpretty.GET, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client.tag_output_columns([COLUMN_NAME])
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_put_column_schema_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    ORG_ID = _get_org()
    MODEL_ID = "xxx"
    COLUMN_NAME = "foo"
    uri = f"{ENDPOINT}/v0/organizations/{ORG_ID}/models/{MODEL_ID}/schema/column/{COLUMN_NAME}"
    httpretty.register_uri(httpretty.PUT, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter(dataset_id=MODEL_ID)

    # monkey patches
    real_get_existing_schema = writer._whylabs_client._get_existing_column_schema
    writer._whylabs_client._get_existing_column_schema = lambda x, y: ColumnSchema("", "", "")

    real_needs_update = writer._whylabs_client._column_schema_needs_update
    writer._whylabs_client._column_schema_needs_update = lambda x, y: True

    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client._put_column_schema(COLUMN_NAME, "bar")

    # unpatch
    writer._whylabs_client._get_existing_column_schema = real_get_existing_schema
    writer._whylabs_client._column_schema_needs_update = real_needs_update

    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_log_async_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    MODEL_ID = "xxx"
    uri = f"{ENDPOINT}/v1/log/async"
    httpretty.register_uri(httpretty.POST, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client.get_upload_url_batch(0)
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_post_log_segmented_reference_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    ORG_ID = _get_org()
    MODEL_ID = "xxx"
    uri = f"{ENDPOINT}/v0/organizations/{ORG_ID}/dataset-profiles/models/{MODEL_ID}/reference-profile"
    httpretty.register_uri(httpretty.POST, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client._get_upload_urls_segmented_reference([], 0, "foo")
    assert exc_info.value.status == 429


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="HTTPretty not supported with this Python version")
@pytest.mark.load
@httpretty.activate(allow_net_connect=False, verbose=True)
def test_post_log_unsegmented_reference_retry():
    ENDPOINT = os.environ["WHYLABS_API_ENDPOINT"]
    ORG_ID = _get_org()
    MODEL_ID = "xxx"
    uri = f"{ENDPOINT}/v0/organizations/{ORG_ID}/log/reference/{MODEL_ID}"
    httpretty.register_uri(httpretty.POST, uri, status=429)  # Fake WhyLabs that throttles

    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    with pytest.raises(ApiException) as exc_info:
        _ = writer._whylabs_client.get_upload_url_unsegmented_reference(0, "foo")
    assert exc_info.value.status == 429


@pytest.mark.load
def test_log_reference():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, allow_local=False)
    data = {"col1": 1, "col2": "foo"}
    trace_id = str(uuid4())
    why.log(data, trace_id=trace_id, name="foo")
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    writer = WhyLabsWriter()
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    download_url = response.get("traces")[0]["download_url"]
    headers = {"Content-Type": "application/octet-stream"}
    downloaded_profile = writer._s3_pool.request("GET", download_url, headers=headers, timeout=writer._timeout_seconds)
    deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
    assert deserialized_view.get_columns().keys() == data.keys()


@pytest.mark.load
def test_log_batch():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, allow_local=False)
    data = {"col1": 1, "col2": "foo"}
    trace_id = str(uuid4())
    why.log(data, trace_id=trace_id)
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    writer = WhyLabsWriter()
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    download_url = response.get("traces")[0]["download_url"]
    headers = {"Content-Type": "application/octet-stream"}
    downloaded_profile = writer._s3_pool.request("GET", download_url, headers=headers, timeout=writer._timeout_seconds)
    deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
    assert deserialized_view.get_columns().keys() == data.keys()


@pytest.mark.load
def test_whylabs_writer():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    schema = DatasetSchema()
    data = {"col1": 1, "col2": "foo"}
    trace_id = str(uuid4())
    result = why.log(data, schema=schema, trace_id=trace_id)
    writer = WhyLabsWriter()
    success, _ = writer.write(result.profile())
    assert success
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    download_url = response.get("traces")[0]["download_url"]
    headers = {"Content-Type": "application/octet-stream"}
    downloaded_profile = writer._s3_pool.request("GET", download_url, headers=headers, timeout=writer._timeout_seconds)
    deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
    assert deserialized_view.get_columns().keys() == data.keys()


@pytest.mark.load
@pytest.mark.parametrize(
    "zipped",
    [
        # (True),  TODO: zip not working yet
        (False)
    ],
)
def test_whylabs_writer_segmented(zipped: bool):
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    schema = DatasetSchema(segments=segment_on_column("col1"))
    data = {"col1": [1, 2, 1, 3, 2, 2], "col2": ["foo", "bar", "wat", "foo", "baz", "wat"]}
    df = pd.DataFrame(data)
    trace_id = str(uuid4())
    profile = why.log(df, schema=schema, trace_id=trace_id)
    writer = WhyLabsWriter()
    success, status = writer.write(profile, zip=zipped)
    assert success
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    assert len(response.get("traces")) == 3
    for trace in response.get("traces"):
        download_url = trace.get("download_url")
        headers = {"Content-Type": "application/octet-stream"}
        downloaded_profile = writer._s3_pool.request(
            "GET", download_url, headers=headers, timeout=writer._timeout_seconds
        )
        deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
        assert deserialized_view.get_columns().keys() == data.keys()


@pytest.mark.load
def test_whylabs_writer_explicit_segmented():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    schema = DatasetSchema(segments=segment_on_column("col1"))
    data = {"col1": [1, 2, 1, 3, 2, 2], "col2": ["foo", "bar", "wat", "foo", "baz", "wat"]}
    df = pd.DataFrame(data)
    trace_id = str(uuid4())
    profile = why.log(df, schema=schema, trace_id=trace_id, segment_key_values={"version": "1.0.0"})

    assert profile.count == 3
    partitions = profile.partitions
    assert len(partitions) == 1
    partition = partitions[0]
    segments = profile.segments_in_partition(partition)
    assert len(segments) == 3

    first_segment = next(iter(segments))
    assert first_segment.key == ("1", "1.0.0")

    writer = WhyLabsWriter()
    success, status = writer.write(profile)
    assert success
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    assert len(response.get("traces")) == 3
    for trace in response.get("traces"):
        download_url = trace.get("download_url")
        headers = {"Content-Type": "application/octet-stream"}
        downloaded_profile = writer._s3_pool.request(
            "GET", download_url, headers=headers, timeout=writer._timeout_seconds
        )
        deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
        assert deserialized_view._metadata["whylogs.tag.version"] == "1.0.0"
        assert deserialized_view.get_columns().keys() == data.keys()


@pytest.mark.load
@pytest.mark.parametrize(
    "segmented,zipped",
    [
        # (True, True),  TODO: zip not working yet
        (True, False),
        (False, False),
    ],
)
def test_whylabs_writer_reference(segmented: bool, zipped: bool):
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    if segmented:
        schema = DatasetSchema(segments=segment_on_column("col1"))
    else:
        schema = DatasetSchema()

    data = {"col1": [1, 2, 1, 3, 2, 2], "col2": ["foo", "bar", "wat", "foo", "baz", "wat"]}
    df = pd.DataFrame(data)
    trace_id = str(uuid4())
    result = why.log(df, schema=schema, trace_id=trace_id)
    writer = WhyLabsWriter().option(reference_profile_name="monty")
    # The test currently fails without use_v0 = !segmented. This may not be the final/intended behavior.
    success, ref_id = writer.write(result, use_v0=not segmented, zip=zipped)
    assert success
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ReferenceProfileItemResponse = dataset_api.get_reference_profile(
        ORG_ID,
        MODEL_ID,
        ref_id,
    )
    download_url = response.get("download_url") or response.get("download_urls")[0]
    headers = {"Content-Type": "application/octet-stream"}
    downloaded_profile = writer._s3_pool.request("GET", download_url, headers=headers, timeout=writer._timeout_seconds)
    deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
    assert deserialized_view.get_columns().keys() == data.keys()


# The following tests assume the platform already has the model


@pytest.mark.load
def test_tag_columns():
    writer = WhyLabsWriter()
    writer.tag_output_columns(["col1"])
    writer.tag_input_columns(["col2"])
    model_api_instance = writer._whylabs_client._get_or_create_models_client()
    col1_schema = writer._whylabs_client._get_existing_column_schema(model_api_instance, "col1")
    assert col1_schema["classifier"] == "output"
    col2_schema = writer._whylabs_client._get_existing_column_schema(model_api_instance, "col2")
    assert col2_schema["classifier"] == "input"

    # swap 'em so we won't accidentally pass from previous state
    writer.tag_output_columns(["col2"])
    writer.tag_input_columns(["col1"])
    col1_schema = writer._whylabs_client._get_existing_column_schema(model_api_instance, "col1")
    assert col1_schema["classifier"] == "input"
    col2_schema = writer._whylabs_client._get_existing_column_schema(model_api_instance, "col2")
    assert col2_schema["classifier"] == "output"


@pytest.mark.load
def test_feature_weights_writer():
    writer = WhyLabsWriter()
    feature_weights = FeatureWeights({"col1": 1.0, "col2": 0.0})
    writer.write_feature_weights(feature_weights)
    retrieved_weights = writer.get_feature_weights()
    assert feature_weights.weights == retrieved_weights.weights

    # swap 'em so we won't accidentally pass from previous state
    feature_weights = FeatureWeights({"col1": 0.0, "col2": 1.0})
    writer.write_feature_weights(feature_weights)
    retrieved_weights = writer.get_feature_weights()
    assert feature_weights.weights == retrieved_weights.weights


@pytest.mark.load
def test_feature_weights_client():
    client = WhyLabsClient()
    feature_weights = FeatureWeights({"col1": 1.0, "col2": 0.0})
    client.write_feature_weights(feature_weights)
    retrieved_weights = client.get_feature_weights()
    assert feature_weights.weights == retrieved_weights.weights

    # swap 'em so we won't accidentally pass from previous state
    feature_weights = FeatureWeights({"col1": 0.0, "col2": 1.0})
    client.write_feature_weights(feature_weights)
    retrieved_weights = client.get_feature_weights()
    assert feature_weights.weights == retrieved_weights.weights


@pytest.mark.load
def test_performance_column():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    writer = WhyLabsWriter()
    status, _ = writer.tag_custom_performance_column("col1", "perf_column", "mean")
    assert status
    model_api = ModelsApi(writer._api_client)
    response: EntitySchema = model_api.get_entity_schema(ORG_ID, MODEL_ID)
    assert (
        response["metrics"]["perf_column"]["column"] == "col1"
        and response["metrics"]["perf_column"]["default_metric"] == "mean"
        and response["metrics"]["perf_column"]["label"] == "perf_column"
    )

    # change it so we won't accidentally pass from previous state
    status, _ = writer.tag_custom_performance_column("col1", "perf_column", "median")
    assert status
    response = model_api.get_entity_schema(ORG_ID, MODEL_ID)
    assert response["metrics"]["perf_column"]["default_metric"] == "median"


@pytest.mark.load
def test_estimation_result():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    result = EstimationResult(0.5)
    writer = WhyLabsWriter()
    trace_id = str(uuid4())
    status, _ = writer.write(result, trace_id=trace_id)
    assert status
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    download_url = response.get("traces")[0]["download_url"]
    headers = {"Content-Type": "application/octet-stream"}
    downloaded_profile = writer._s3_pool.request("GET", download_url, headers=headers, timeout=writer._timeout_seconds)
    deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
    estimation_magic_string = _uncompound_performance_estimation_magic_string()
    assert set(deserialized_view.get_columns().keys()) == {f"{estimation_magic_string}accuracy"}


@pytest.mark.load
def test_transactions():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    schema = DatasetSchema()
    data = {"col1": 1, "col2": "foo"}
    trace_id = str(uuid4())
    result = why.log(data, schema=schema, trace_id=trace_id)
    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    assert writer._whylabs_client._transaction_id is None
    transaction_id = writer.start_transaction()
    assert writer._whylabs_client._transaction_id is not None
    assert writer._whylabs_client._transaction_id == transaction_id
    writer.start_transaction(transaction_id)
    assert writer._whylabs_client._transaction_id == transaction_id
    status, id = writer.write(result)
    writer.commit_transaction()
    assert writer._whylabs_client._transaction_id is None
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    download_url = response.get("traces")[0]["download_url"]
    headers = {"Content-Type": "application/octet-stream"}
    downloaded_profile = writer._s3_pool.request("GET", download_url, headers=headers, timeout=writer._timeout_seconds)
    deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
    assert deserialized_view.get_columns().keys() == data.keys()


@pytest.mark.load
def test_transaction_aborted():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    data = {"col1": 1, "col2": "foo"}
    trace_id = str(uuid4())
    result = why.log(data, trace_id=trace_id)
    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    transaction_id = writer.start_transaction()
    status, id = writer.write(result)
    assert status
    writer.abort_transaction()
    status, id = writer.write(result)
    assert transaction_id == writer._whylabs_client._transaction_id
    with pytest.raises(TransactionAbortedException) as e:
        writer.commit_transaction()
        assert str(e) == "Transaction has been aborted"

    assert writer._whylabs_client._transaction_id is None
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    assert len(response.get("traces")) == 0


@pytest.mark.load
def test_transaction_context():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    schema = DatasetSchema()
    csv_url = "https://whylabs-public.s3.us-west-2.amazonaws.com/datasets/tour/current.csv"
    df = pd.read_csv(csv_url)
    pdfs = np.array_split(df, 7)
    tids = list()
    with WhyLabsTransactionWriter() as writer:
        assert writer.transaction_id is not None
        for data in pdfs:
            trace_id = str(uuid4())
            tids.append(trace_id)
            result = why.log(data, schema=schema, trace_id=trace_id)
            status, id = writer.write(result.profile())
            if not status:
                raise Exception()  # or retry the profile...
        status = writer.transaction_status()
        assert len(status["files"]) == len(pdfs)

    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    for trace_id in tids:
        response: ProfileTracesResponse = dataset_api.get_profile_traces(
            org_id=ORG_ID,
            dataset_id=MODEL_ID,
            trace_id=trace_id,
        )
        download_url = response.get("traces")[0]["download_url"]
        headers = {"Content-Type": "application/octet-stream"}
        downloaded_profile = writer._s3_pool.request(
            "GET", download_url, headers=headers, timeout=writer._timeout_seconds
        )
        deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
        assert deserialized_view is not None


@pytest.mark.load
def test_old_transaction_context():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    schema = DatasetSchema()
    csv_url = "https://whylabs-public.s3.us-west-2.amazonaws.com/datasets/tour/current.csv"
    df = pd.read_csv(csv_url)
    pdfs = np.array_split(df, 7)
    tids = list()
    writer = WhyLabsWriter()
    with WhyLabsTransaction(writer):
        assert writer._whylabs_client._transaction_id is not None
        for data in pdfs:
            trace_id = str(uuid4())
            tids.append(trace_id)
            result = why.log(data, schema=schema, trace_id=trace_id)
            status, id = writer.write(result.profile())
            if not status:
                raise Exception()  # or retry the profile...
        status = writer.transaction_status()
        assert len(status["files"]) == len(pdfs)

    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    for trace_id in tids:
        response: ProfileTracesResponse = dataset_api.get_profile_traces(
            org_id=ORG_ID,
            dataset_id=MODEL_ID,
            trace_id=trace_id,
        )
        download_url = response.get("traces")[0]["download_url"]
        headers = {"Content-Type": "application/octet-stream"}
        downloaded_profile = writer._s3_pool.request(
            "GET", download_url, headers=headers, timeout=writer._timeout_seconds
        )
        deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
        assert deserialized_view is not None


@pytest.mark.load
def test_transaction_context_aborted():
    why.init(reinit=True, force_local=True)
    csv_url = "https://whylabs-public.s3.us-west-2.amazonaws.com/datasets/tour/current.csv"
    df = pd.read_csv(csv_url)
    pdfs = np.array_split(df, 7)
    with pytest.raises(TransactionAbortedException):
        with WhyLabsTransactionWriter() as writer:
            transaction_id = writer.transaction_id
            for data, i in zip(pdfs, range(len(pdfs))):
                result = why.log(data)
                if i == 3:
                    writer._whylabs_client.abort_transaction(transaction_id)
                writer.write(result)

    assert writer._whylabs_client._transaction_id is None


@pytest.mark.load
def test_transaction_segmented():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    schema = DatasetSchema(segments=segment_on_column("Gender"))
    csv_url = "https://whylabs-public.s3.us-west-2.amazonaws.com/datasets/tour/current.csv"
    data = pd.read_csv(csv_url)
    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    trace_id = str(uuid4())
    try:
        writer.start_transaction()
        result = why.log(data, schema=schema, trace_id=trace_id)
        status, id = writer.write(result)
        if not status:
            raise Exception()  # or retry the profile...

    except Exception:
        # The start_transaction() or commit_transaction() in the
        # WhyLabsTransaction context manager will throw on failure.
        # Or retry the commit
        logger.exception("Logging transaction failed")

    writer.commit_transaction()
    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    assert len(response.get("traces")) == 2
    for trace in response.get("traces"):
        download_url = trace.get("download_url")
        headers = {"Content-Type": "application/octet-stream"}
        downloaded_profile = writer._s3_pool.request(
            "GET", download_url, headers=headers, timeout=writer._timeout_seconds
        )
        assert downloaded_profile is not None


@pytest.mark.load
def test_transaction_distributed():
    ORG_ID = _get_org()
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(reinit=True, force_local=True)
    schema = DatasetSchema()
    csv_url = "https://whylabs-public.s3.us-west-2.amazonaws.com/datasets/tour/current.csv"
    df = pd.read_csv(csv_url)
    pdfs = np.array_split(df, 7)
    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    tids = list()
    try:
        transaction_id = writer.start_transaction()
        for data in pdfs:  # pretend each iteration is run on a different machine
            dist_writer = WhyLabsWriter(dataset_id=MODEL_ID)
            dist_writer.start_transaction(transaction_id)
            trace_id = str(uuid4())
            tids.append(trace_id)
            result = why.log(data, schema=schema, trace_id=trace_id)
            status, id = dist_writer.write(result.profile())
            if not status:
                raise Exception()  # or retry the profile...
        writer.commit_transaction()
    except Exception:
        logger.exception("Logging transaction failed")

    time.sleep(SLEEP_TIME)  # platform needs time to become aware of the profile
    dataset_api = DatasetProfileApi(writer._api_client)
    assert len(tids) == 7
    for trace_id in tids:
        response: ProfileTracesResponse = dataset_api.get_profile_traces(
            org_id=ORG_ID,
            dataset_id=MODEL_ID,
            trace_id=trace_id,
        )
        download_url = response.get("traces")[0]["download_url"]
        headers = {"Content-Type": "application/octet-stream"}
        downloaded_profile = writer._s3_pool.request(
            "GET", download_url, headers=headers, timeout=writer._timeout_seconds
        )
        deserialized_view = DatasetProfileView.deserialize(downloaded_profile.data)
        assert deserialized_view is not None
