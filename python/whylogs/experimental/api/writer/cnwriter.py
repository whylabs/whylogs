from typing import Any, Optional, Tuple

from whylabs.api.writer.whylabs import WhyLabsWriter
from whylabs.experimental.extras import ConfigStore
from whylabs_client import ApiClient

from whylogs.api.writer.writer import Writable
from whylogs.core.constraints import ConstraintsBuilder
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.utils import deprecated_alias
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.experimental.api.constraints import ConstraintTranslator


class CnWriter(WhyLabsWriter):
    """
    This is WhyLabsWriter that runs any constraint suite for the profile
    and adds the report as columns that can be monitored in WhyLabs
    """

    def __init__(
        self,
        org_id: Optional[str] = None,
        api_key: Optional[str] = None,
        dataset_id: Optional[str] = None,
        api_client: Optional[ApiClient] = None,
        ssl_ca_cert: Optional[str] = None,
        _timeout_seconds: Optional[float] = None,
        constraint_store: Optional[ConfigStore] = None,
    ):
        if constraint_store is not None and not isinstance(constraint_store, ConfigStore):
            raise ValueError("constraint_store must be a subclass of ConfigStore")

        self._cn_store = constraint_store
        super().__init__(org_id, api_key, dataset_id, api_client, ssl_ca_cert, _timeout_seconds)

    @deprecated_alias(profile="file")
    def write(self, file: Writable, **kwargs: Any) -> Tuple[bool, str]:
        if (
            isinstance(file, (DatasetProfile, DatasetProfileView))
            and self._cn_store is not None
            and self._cn_store.exists()
        ):
            constraints_str = self._cn_store.get_latest()
            constraints = ConstraintTranslator().read_constraints_from_yaml(input_str=constraints_str)
            builder = ConstraintsBuilder(file)
            builder.add_constraints(constraints)
            file += builder.whylabs_report(file)  # create report columns and append to profile

        return super().write(file, **kwargs)
