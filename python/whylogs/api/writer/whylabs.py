import os
from typing import Optional

from whylogs.api.writer import Writer
from whylogs.core import DatasetProfileView


class WhyLabsWriter(Writer):
    def __init__(self, org_id: Optional[str] = None, api_key: Optional[str] = None):
        self._org_id = org_id or os.environ.get("WHYLABS_ORG_ID")
        self._api_key = api_key or os.environ.get("WHYLABS_API_KEY")

    def option(self, org_id: Optional[str] = None, api_key: Optional[str] = None) -> "WhyLabsWriter":  # type: ignore
        if org_id is not None:
            self._org_id = org_id
        if api_key is not None:
            self._api_key = api_key

        return self

    def write(self, profile: DatasetProfileView, dataset_id: Optional[str] = None) -> None:
        raise ValueError("Not implemented yet")
