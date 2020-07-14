"""
"""
import pandas as pd
from pandas.testing import assert_frame_equal
import os
import pytest
import json
import shutil
import functools

from whylabs.logs.app import config, session
from whylabs.logs.core.datasetprofile import dataframe_profile
from whylabs.logs.core import datasetprofile
from whylabs.logs.util.protobuf import message_to_json

MY_DIR = os.path.realpath(os.path.dirname(__file__))
TEST_OUTPUT_DIR = os.path.join(MY_DIR, 'test_output', 'diskhandler')
TIMESTAMP_MS = 1593710000000
num_failed = 0


def count_failure(func):
    """
    Keep track of the number of failed test functions called
    """
    @functools.wraps(func)
    def func_wrapper(*args, **kwargs):
        global num_failed
        try:
            out = func(*args, **kwargs)
        except Exception as e:
            num_failed += 1
            raise e
        return out
    return func_wrapper


@pytest.fixture(scope='module')
def dataset_profile_data(df_lending_club):
    print('Generating dataset profile data')
    df = df_lending_club
    # Generate dataframe profile
    prof = dataframe_profile(df)
    # Generate output formats
    protobuf = prof.to_protobuf()
    summary = prof.to_summary()
    flat_summary = datasetprofile.flatten_summary(summary)
    json_summary = message_to_json(summary)

    output_dir = os.path.join(TEST_OUTPUT_DIR, 'whylogs')
    handler = session.DiskHandler(folder=output_dir, team='team',
                                  pipeline='datapipe', user='user')

    yield {
        'protobuf': protobuf,
        'flat_summary': flat_summary,
        'json': json_summary,
        'dataset_profile': prof,
        'output_dir': output_dir,
        'logger_name': 'logger.name',
        'handler': handler,
    }

    if num_failed == 0:
        # Clean up generated files on success only!
        shutil.rmtree(output_dir)


@count_failure
def test_handle_flat(dataset_profile_data):
    logger_name = dataset_profile_data['logger_name']
    handler = dataset_profile_data['handler']

    # Generate outputs
    flat_response = handler.handle_flat(
        dataset_profile_data['flat_summary'],
        logger_name,
        'dataset_summary',
        TIMESTAMP_MS,
    )
    print(flat_response.dest)

    # Load output
    flat_table = pd.read_csv(flat_response.dest['flat_table'])
    hist = json.load(open(flat_response.dest['histogram'], 'rt'))
    strings = json.load(open(flat_response.dest['freq_strings'], 'rt'))
    numbers = json.load(open(flat_response.dest['freq_numbers'], 'rt'))

    # Validate output
    assert hist == dataset_profile_data['flat_summary']['hist']
    assert numbers == dataset_profile_data['flat_summary']['frequent_numbers']
    assert strings == dataset_profile_data['flat_summary']['frequent_strings']
    assert_frame_equal(
        flat_table, dataset_profile_data['flat_summary']['summary'],
    )


@count_failure
def test_handle_json(dataset_profile_data):
    logger_name = dataset_profile_data['logger_name']
    handler = dataset_profile_data['handler']
    json_response = handler.handle_json(
        dataset_profile_data['json'],
        logger_name,
        'dataset_summary',
        TIMESTAMP_MS,
    )
    print(json_response.dest)

    # Read outputs
    with open(json_response.dest, 'rt') as fp:
        json_text = fp.read()
    summary_dict = json.loads(json_text)
    # Validate summary
    assert summary_dict == json.loads(dataset_profile_data['json'])


@count_failure
def test_handle_protobuf(dataset_profile_data):
    logger_name = dataset_profile_data['logger_name']
    handler = dataset_profile_data['handler']

    proto_response = handler.handle_protobuf(
        dataset_profile_data['protobuf'],
        logger_name,
        'dataset_profile',
        TIMESTAMP_MS,
    )
    print(proto_response.dest)

    # Read outputs
    with open(proto_response.dest, 'rb') as fp:
        proto_bytes = fp.read()
    prof = datasetprofile.DatasetProfile.from_protobuf_string(proto_bytes)

    # Validate outputs
    prof1 = dataset_profile_data['dataset_profile']
    assert set(prof.columns.keys()) == set(prof1.columns.keys())
