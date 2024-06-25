import datetime
import logging
from typing import Any, List, Optional, Tuple, Union

from whylabs_client import ApiClient

from whylogs.api.logger import log
from whylogs.api.whylabs.session.session_manager import INIT_DOCS
from whylogs.api.writer.whylabs_base import WhyLabsWriterBase
from whylogs.api.writer.whylabs_client import WhyLabsClient
from whylogs.api.writer.writer import _Writable
from whylogs.core.utils import deprecated_alias
from whylogs.experimental.performance_estimation.estimation_results import (
    EstimationResult,
)
from whylogs.migration.uncompound import (
    _uncompound_performance_estimation_feature_flag,
    _uncompound_performance_estimation_magic_string,
)

logger = logging.getLogger(__name__)


class WhyLabsEstimationResultWriter(WhyLabsWriterBase):
    f"""
    A WhyLogs writer to upload EstimationResults onto the WhyLabs platform.

    >**IMPORTANT**: In order to correctly send your profiles over, make sure you have
    the following environment variables set: `[WHYLABS_ORG_ID, WHYLABS_API_KEY, WHYLABS_DEFAULT_DATASET_ID]`. You
    can also follow the authentication instructions for the why.init() method at {INIT_DOCS}.
    It is highly recommended you don't persist credentials in code!

    You shouldn't have to supply these parameters to the writer in practice. You should depend on why.init() to resolve
    the credentials for you. These are here for one-offs and testing convenience.

    Parameters
    ----------
    org_id: str, optional
        Your organization ID on the WhyLabs platform. If not informed, will get the mentioned environment
        variable instead.

    api_key: str, optional
        The API Key to authenticate with WhyLabs. If not informed, will get the mentioned environment
        variable instead.

    dataset_id: str, optional
        The Dataset ID, often referred to as model ID, that will tie the profile to your tracked data.
        If not informed, will get the mentioned environment variable instead.
    Returns
    -------
        None

    Examples
    --------
    An example usage of this method can be represented with the simple code above. Here we
    are assuming the user has already assigned a pandas.DataFrame to the `df` variable.

    ```python
     ```

    """

    def __init__(
        self,
        org_id: Optional[str] = None,
        api_key: Optional[str] = None,
        dataset_id: Optional[str] = None,
        api_client: Optional[ApiClient] = None,
        ssl_ca_cert: Optional[str] = None,
        _timeout_seconds: Optional[float] = None,
        whylabs_client: Optional[WhyLabsClient] = None,
    ):
        super().__init__(org_id, api_key, dataset_id, api_client, ssl_ca_cert, _timeout_seconds, whylabs_client)

    @deprecated_alias(profile="file")
    def write(
        self, file: _Writable, dest: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        if not isinstance(file, EstimationResult):
            raise ValueError("I can only write EstimationResult")

        if _uncompound_performance_estimation_feature_flag():
            trace_id = kwargs.get("trace_id")
            estimation_magic_string = _uncompound_performance_estimation_magic_string()
            estimation_result_profile = log(
                {f"{estimation_magic_string}accuracy": file.accuracy}, trace_id=trace_id
            ).profile()
            timestamp = file.target_result_timestamp or datetime.datetime.now(datetime.timezone.utc)
            estimation_result_profile.set_dataset_timestamp(timestamp)
            view = estimation_result_profile.view()
            return view.writer(
                "whylabs",
                org_id=self._whylabs_client._org_id,
                api_key=self._whylabs_client._api_key,
                dataset_id=self._whylabs_client._dataset_id,
                api_client=None,  # TODO: support custom clients
                ssl_ca_cert=self._whylabs_client._ssl_ca_cert,
                _timeout_seconds=self._whylabs_client._timeout_seconds,
            ).write(dest, **kwargs)

        return False, str("Performance estimation feature flag is not enabled")
