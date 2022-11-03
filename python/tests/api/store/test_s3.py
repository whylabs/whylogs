import boto3
import pytest
from botocore.client import BaseClient

from whylogs.api.store import ProfileNameQuery
from whylogs.api.store.s3 import S3Store
from whylogs.core import DatasetProfileView

# TODO mock s3 bucket

BUCKET_NAME = "murilo-test-bucket"
STORE_PROFILE_NAME = "my_object"
S3_PROFILE_NAME = ""  # get on aws sso login


@pytest.fixture
def s3_client() -> BaseClient:
    session = boto3.Session(profile_name=S3_PROFILE_NAME)
    client = session.client("s3")
    return client


@pytest.fixture
def store(s3_client):
    store = S3Store(bucket_name=BUCKET_NAME, s3_client=s3_client)
    return store


def test_s3_store_lists_written_profiles(store):
    store_list = store.list()
    assert isinstance(store_list, list)
    assert store_list[0] == STORE_PROFILE_NAME


def test_s3_store_gets_files_with_name_query(store):
    query = ProfileNameQuery(profile_name=STORE_PROFILE_NAME)
    profile = store.get(query=query)
    assert profile is not None
    assert isinstance(profile, DatasetProfileView)


def test_s3_store_gets_files_with_date_query():
    pass
