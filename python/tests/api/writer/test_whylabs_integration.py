import logging
import os
import time
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from whylabs_client.api.dataset_profile_api import DatasetProfileApi
from whylabs_client.api.models_api import ModelsApi
from whylabs_client.model.entity_schema import EntitySchema
from whylabs_client.model.profile_traces_response import ProfileTracesResponse
from whylabs_client.model.reference_profile_item_response import (
    ReferenceProfileItemResponse,
)

import whylogs as why
from whylogs.api.writer.whylabs import WhyLabsTransaction, WhyLabsWriter
from whylogs.core import DatasetProfileView
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column

# TODO: These won't work well if multiple tests run concurrently

os.environ["WHYLOGS_NO_ANALYTICS"] = "True"
# WHYLABS_API_ENDPOINT, WHYLABS_API_KEY, WHYLABS_DEFAULT_ORG_ID, and
# WHYLABS_DEFAULT_DATASET_ID need to come from the environment


SLEEP_TIME = 30

logger = logging.getLogger(__name__)


@pytest.mark.load
def test_whylabs_writer():
    ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID")
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(force_local=True)
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
@pytest.mark.parametrize("zipped", [(True), (False)])
def test_whylabs_writer_segmented(zipped: bool):
    ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID")
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(force_local=True)
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
@pytest.mark.parametrize("segmented,zipped", [(True, True), (True, False), (False, False)])
def test_whylabs_writer_reference(segmented: bool, zipped: bool):
    ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID")
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(force_local=True)
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
def test_feature_weights():
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
def test_performance_column():
    ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID")
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    writer = WhyLabsWriter()
    status, _ = writer.tag_custom_performance_column("col1", "perf column", "mean")
    assert status
    model_api = ModelsApi(writer._api_client)
    response: EntitySchema = model_api.get_entity_schema(ORG_ID, MODEL_ID)
    assert (
        response["metrics"]["perf_column"]["column"] == "col1"
        and response["metrics"]["perf_column"]["default_metric"] == "mean"
        and response["metrics"]["perf_column"]["label"] == "perf column"
    )

    # change it so we won't accidentally pass from previous state
    status, _ = writer.tag_custom_performance_column("col1", "perf column", "median")
    assert status
    response = model_api.get_entity_schema(ORG_ID, MODEL_ID)
    assert response["metrics"]["perf_column"]["default_metric"] == "median"


@pytest.mark.load
def test_estimation_result():
    # TODO: WhyLabsWriter::write_estimation_result() needs a trace id
    pass


@pytest.mark.load
def test_transactions():
    ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID")
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(force_local=True)
    schema = DatasetSchema()
    data = {"col1": 1, "col2": "foo"}
    trace_id = str(uuid4())
    result = why.log(data, schema=schema, trace_id=trace_id)
    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    assert writer._transaction_id is None
    transaction_id = writer.start_transaction()
    assert writer._transaction_id is not None
    assert writer._transaction_id == writer._whylabs_client._transaction_id == transaction_id
    writer.start_transaction(transaction_id)
    status, id = writer.write(result)
    writer.commit_transaction()
    assert writer._transaction_id is None
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
def test_transaction_context():
    ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID")
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(force_local=True)
    schema = DatasetSchema()
    csv_url = "https://whylabs-public.s3.us-west-2.amazonaws.com/datasets/tour/current.csv"
    df = pd.read_csv(csv_url)
    pdfs = np.array_split(df, 7)
    writer = WhyLabsWriter(dataset_id=MODEL_ID)
    tids = list()
    try:
        with WhyLabsTransaction(writer):
            assert writer._transaction_id is not None
            assert writer._whylabs_client._transaction_id == writer._transaction_id
            for data in pdfs:
                trace_id = str(uuid4())
                tids.append(trace_id)
                result = why.log(data, schema=schema, trace_id=trace_id)
                status, id = writer.write(result.profile())
                if not status:
                    raise Exception()  # or retry the profile...

    except Exception:
        # The start_transaction() or commit_transaction() in the
        # WhyLabsTransaction context manager will throw on failure.
        # Or retry the commit
        logger.exception("Logging transaction failed")

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
def test_transaction_segmented():
    ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID")
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(force_local=True)
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
    ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID")
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(force_local=True)
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
