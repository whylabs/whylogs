import logging
from typing import Iterator, List, Optional, Union

from boto3 import client
from botocore.client import BaseClient

from whylogs.api.reader.s3 import S3Reader
from whylogs.api.store import ProfileStore
from whylogs.api.store.query import DateQuery, ProfileNameQuery
from whylogs.core import DatasetProfile, DatasetProfileView

logger = logging.getLogger(__name__)
BASE_PREFIX = "profile_store/"


class S3Store(ProfileStore):
    def __init__(self, bucket_name: str, s3_client: BaseClient, s3_reader: Optional[S3Reader] = None):
        self.bucket_name = bucket_name
        self.s3_client = s3_client or client("s3")
        self.s3_reader = s3_reader or S3Reader(s3_client=self.s3_client)

    def _list_objects(self) -> Iterator[str]:
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=BASE_PREFIX, Delimiter="/")
        for content in response.get("CommonPrefixes", []):
            yield content.get("Prefix")

    def list(self) -> List:
        profiles_list = []
        for object in self._list_objects():
            profiles_list.append(object.rsplit("/", 2)[1])
        return profiles_list

    def _get_keys_from_prefix(self, prefix: str):
        paginator = self.s3_client.get_paginator("list_objects_v2")
        keys_list = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, Delimiter="/"):
            keys = [content["Key"] for content in page.get("Contents")]
            keys_list.extend(keys)
        return keys_list

    def get(self, query: Union[ProfileNameQuery, DateQuery]) -> DatasetProfileView:
        logger.debug("Fetching profiles with specified StoreQuery...")
        if isinstance(query, ProfileNameQuery):
            available_profiles = self.list()
            if query.profile_name in available_profiles:
                keys_list = self._get_keys_from_prefix(f"{BASE_PREFIX + query.profile_name}/")
                profiles_list = [f for f in keys_list if f.endswith(".bin")]
                profile_view = DatasetProfile().view()
                for profile_path in profiles_list:
                    try:
                        profile_result = self.s3_reader.option(
                            bucket_name=self.bucket_name, object_name=profile_path
                        ).read()
                        profile_view = profile_view.merge(profile_result.view())
                        logger.debug(f"Profile {profile_path} successfully read")
                    except Exception as e:
                        logger.error("Could not read the file {}".format(e))
                return profile_view
            else:
                logger.error("Queried profile does not exist!")

    def write(self):
        pass
