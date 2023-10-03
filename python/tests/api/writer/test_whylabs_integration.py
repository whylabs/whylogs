import os
import time
from uuid import uuid4

import pandas as pd
import pytest
from whylabs_client.api.dataset_profile_api import DatasetProfileApi
from whylabs_client.model.profile_traces_response import ProfileTracesResponse

import whylogs as why
from whylogs.api.writer.whylabs import WhyLabsWriter
from whylogs.core import DatasetProfileView
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column

# TODO: These won't work well if multiple tests run concurrently

os.environ["WHYLOGS_NO_ANALYTICS"] = "True"
# WHYLABS_API_ENDPOINT, WHYLABS_API_KEY, WHYLABS_DEFAULT_ORG_ID, and
# WHYLABS_DEFAULT_DATASET_ID need to come from the environment


SLEEP_TIME = 30


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
    writer.write(result.profile())
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
def test_whylabs_writer_segmented():
    ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID")
    MODEL_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
    why.init(force_local=True)
    schema = DatasetSchema(segments=segment_on_column("col1"))
    data = {"col1": [1, 2, 1, 3, 2, 2], "col2": ["foo", "bar", "wat", "foo", "baz", "wat"]}
    df = pd.DataFrame(data)
    trace_id = str(uuid4())
    result = why.log(df, schema=schema, trace_id=trace_id)
    writer = WhyLabsWriter()
    writer.write(result)
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
@pytest.mark.parametrize("segmented", [(True), (False)])
def test_whylabs_writer_reference(segmented: bool):
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
    writer.write(result)
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


# The following tests assume the platform already has the model


@pytest.mark.load
def test_tag_columns():
    writer = WhyLabsWriter()
    writer.tag_output_columns(["col1"])
    writer.tag_input_columns(["col2"])
    model_api_instance = writer._get_or_create_models_client()
    col1_schema = writer._get_existing_column_schema(model_api_instance, "col1")
    assert col1_schema["classifier"] == "output"
    col2_schema = writer._get_existing_column_schema(model_api_instance, "col2")
    assert col2_schema["classifier"] == "input"

    # swap 'em so we won't accidentally pass from previous state
    writer.tag_output_columns(["col2"])
    writer.tag_input_columns(["col1"])
    col1_schema = writer._get_existing_column_schema(model_api_instance, "col1")
    assert col1_schema["classifier"] == "input"
    col2_schema = writer._get_existing_column_schema(model_api_instance, "col2")
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
@pytest.mark.xfail(raises=AttributeError, reason="writer calls non-existant function")
def test_performance_column():
    writer = WhyLabsWriter()
    _, res = writer.tag_custom_performance_column("col1", "perf column", "mean")
    assert res == "OK"
    model_api_instance = writer._get_or_create_models_client()
    col1_schema = writer._get_existing_column_schema(model_api_instance, "col1")
    assert col1_schema["label"] == "perf column"
    assert col1_schema["default_metric"] == "mean"


@pytest.mark.load
def test_estimation_result():
    # TODO: WhyLabsWriter::write_estimation_result() needs a trace id
    pass
