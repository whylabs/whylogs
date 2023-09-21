import os
import pytest
from uuid import uuid4

from whylabs_client.api.dataset_profile_api import DatasetProfileApi

import whylogs as why
from whylogs.api.writer.whylabs import WhyLabsWriter
from whylogs.core import DatasetProfileView
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.schema import DatasetSchema

# TODO: These won't work well if multiple tests run concurrently

# TODO: Aim this at an org with granular profiles
ORG_ID = "org-BDw3Jt"
API_KEY = "KyaubnkdlK....:org-BDw3Jt"
MODEL_ID = "model-2"

os.environ['WHYLOGS_NO_ANALYTICS']='True'
os.environ["WHYLABS_API_ENDPOINT"] = "https://songbird.development.whylabsdev.com"
os.environ["WHYLABS_DEFAULT_ORG_ID"] = ORG_ID
#os.environ["WHYLABS_API_KEY"] = API_KEY
os.environ["WHYLABS_DEFAULT_DATASET_ID"] = MODEL_ID


@pytest.mark.load
def test_whylabs_writer():
    why.init(force_local=True)
    schema = DatasetSchema()
    data = {"col1": 1, "col2": "foo"}
    trace_id = str(uuid4())
    result = why.log(data, schema=schema, trace_id=trace_id)
    writer = WhyLabsWriter()
    writer.write(result.profile())
    dataset_api = DatasetProfileApi(writer._api_client)
    response: ProfileTracesResponse = dataset_api.get_profile_traces(
        org_id=ORG_ID,
        dataset_id=MODEL_ID,
        trace_id=trace_id,
    )
    """
    download_url1 = response.get("traces")[0]["download_url"]
    headers = {"Content-Type": "application/octet-stream"}
    downloaded_profile1 = writer._s3_pool.request("GET", download_url1, headers=headers, timeout=writer._timeout_seconds)
    deserialized_view1 = DatasetProfileView.deserialize(downloaded_profile1.data)
    assert deserialized_view1 == result.profile().view()
    """


# TODO: test writing segmented profile, [Segmented]ResultSet, [un]segmented reference profiles


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
def test_performance_column():
    pass


@pytest.mark.load
def test_estimation_result():
    pass
