import datetime
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional, Tuple, Union

import requests  # type: ignore
from whylabs_client import ApiClient
from whylabs_client.model.segment_tag import SegmentTag

from whylogs.api.logger.result_set import ProfileResultSet, ResultSet, ViewResultSet
from whylogs.api.writer.whylabs_client import WhyLabsClient
from whylogs.api.writer.writer import Writer, _Writable
from whylogs.core import DatasetProfileView
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.errors import BadConfigError
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
from whylogs.migration.uncompound import (
    FeatureFlags,
    _uncompound_dataset_profile,
    _uncompound_metric_feature_flag,
)

FIVE_MINUTES_IN_SECONDS = 60 * 5
DAY_IN_SECONDS = 60 * 60 * 24
FIVE_YEARS_IN_SECONDS = DAY_IN_SECONDS * 365 * 5
_WHYLABS_SKIP_CONFIG_READ = os.environ.get("_WHYLABS_SKIP_CONFIG_READ") or False

logger = logging.getLogger(__name__)


def _check_whylabs_condition_count_uncompound() -> bool:
    global _WHYLABS_SKIP_CONFIG_READ
    if _WHYLABS_SKIP_CONFIG_READ:
        return True
    whylabs_config_url = (
        "https://whylabs-public.s3.us-west-2.amazonaws.com/whylogs_config/whylabs_condition_count_disabled"
    )
    logger.info(f"checking: {whylabs_config_url}")
    try:
        response = requests.head(whylabs_config_url)
        logger.info(f"checking: {whylabs_config_url}")
        logger.info(f"headers are: {response.headers} code: {response.status_code}")
        if response.status_code == 200:
            logger.info(
                "found the whylabs condition count disabled file so running uncompound on condition count metrics"
            )
            return True
        elif response.status_code == 404:
            logger.info("no whylabs condition count disabled so sending condition count metrics as v1.")
            return False
        else:
            logger.info(f"Got response code {response.status_code} but expected 200, so running uncompound")
    except Exception:
        pass

    _WHYLABS_SKIP_CONFIG_READ = True
    return True


class WhyLabsWriterBase(Writer):
    """
    Base class for WhyLabs writers
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
        self._whylabs_client: WhyLabsClient = whylabs_client or WhyLabsClient(
            org_id,
            api_key,
            dataset_id,
            api_client,
            ssl_ca_cert,
            _timeout_seconds,
        )

    @property
    def _api_client(self) -> ApiClient:
        if self._whylabs_client._api_client is None:
            self._whylabs_client._refresh_client()
        return self._whylabs_client._api_client

    def transaction_status(self) -> Dict[str, Any]:
        id = self._whylabs_client._transaction_id  # type: ignore
        if id is None:
            raise ValueError("No transaction is active")
        return self._whylabs_client.transaction_status(id)

    @property
    def _s3_pool(self):
        return self._whylabs_client._s3_pool

    @property
    def _timeout_seconds(self) -> float:
        return self._whylabs_client._timeout_seconds

    @property
    def key_id(self) -> str:
        return self._whylabs_client.key_id

    def check_interval(self, interval_seconds: int) -> None:
        if interval_seconds < FIVE_MINUTES_IN_SECONDS:
            raise BadConfigError("Bad WhyLabsWriter config: interval must be greater or equal to five minutes")

    def option(self, **kwargs) -> Writer:  # type: ignore
        """

        Parameters
        ----------
        org_id the organization ID
        dataset_id the dataset Id
        api_key the API key
        reference_profile_name the name of the reference profile
        configuration the additional configuration for the REST client
        transaction_id to ID of currently active transaction

        org_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        api_key: Optional[str] = None,
        reference_profile_name: Optional[str] = None,
        configuration: Optional[Configuration] = None,
        transaction_id: Optional[str] = None,
        ssl_ca_cert: Optional[str] = None,
        api_client: Optional[ApiClient] = None,
        timeout_seconds: Optional[float] = None,
        prefer_sync: Optional[bool] = None,

        Returns a "WhyLabsWriter" with these options configured
        -------

        """
        self._whylabs_client = self._whylabs_client.option(**kwargs)
        return self

    def _get_dataset_epoch(
        self, view: Union[DatasetProfileView, SegmentedDatasetProfileView], utc_now: Optional[datetime.datetime] = None
    ) -> int:
        utc_now = utc_now or datetime.datetime.now(datetime.timezone.utc)
        dataset_timestamp = view.dataset_timestamp or utc_now
        stamp = dataset_timestamp.timestamp()
        time_delta_seconds = utc_now.timestamp() - stamp
        if time_delta_seconds < 0:
            logger.warning(
                f"About to upload a profile with a dataset_timestamp that is in the future: "
                f"{time_delta_seconds}s old."
            )
        if time_delta_seconds > FIVE_YEARS_IN_SECONDS:
            logger.error(
                f"A profile being uploaded to WhyLabs has a dataset_timestamp of({dataset_timestamp}) "
                f"compared to current datetime: {utc_now}. Uploads of profiles older than 5 years "
                "might not be monitored in WhyLabs and may take up to 24 hours to show up."
            )

        if stamp <= 0:
            logger.error(
                f"Profiles should have timestamps greater than 0, but found a timestamp of {stamp}"
                f" and current timestamp is {utc_now.timestamp()}, this is likely an error."
            )

        return int(stamp * 1000)

    def _custom_tagging(
        self,
        view: Union[DatasetProfile, DatasetProfileView, SegmentedDatasetProfileView, ResultSet],
    ) -> None:
        self._whylabs_client._tag_custom_perf_metrics(view)
        self._whylabs_client._tag_custom_output_metrics(view)

    def _prepare_view_for_upload(
        self,
        view: Union[DatasetProfileView, SegmentedDatasetProfileView],
    ) -> Union[DatasetProfileView, SegmentedDatasetProfileView]:
        view_is_a_segment = isinstance(view, SegmentedDatasetProfileView)
        flags = FeatureFlags(_check_whylabs_condition_count_uncompound())
        if _uncompound_metric_feature_flag():
            if view_is_a_segment:
                updated_profile_view = _uncompound_dataset_profile(view.profile_view, flags)
                view = SegmentedDatasetProfileView(
                    profile_view=updated_profile_view, segment=view._segment, partition=view._partition
                )

            else:
                view = _uncompound_dataset_profile(view, flags)

        return view

    def _send_writable_to_whylabs(
        self,
        view: Union[DatasetProfileView, SegmentedDatasetProfileView],
        profile_id: str,
        upload_url: str,
        dataset_timestamp_epoch: Optional[int] = None,
        whylabs_tags: Optional[List[SegmentTag]] = None,
        **kwargs: Any,
    ) -> Tuple[bool, str]:
        has_performance_metrics = view.model_performance_metrics
        dataset_timestamp_epoch = dataset_timestamp_epoch or self._get_dataset_epoch(view)
        with tempfile.NamedTemporaryFile() as tmp_file:
            # currently whylabs is not ingesting the v1 format of segmented profiles as segmented
            # so we default to sending them as v0 profiles if the override `use_v0` is not defined,
            # if `use_v0` is defined then pass that through to control the serialization format.

            # TODO: the use_v0 logic looks backwards
            if has_performance_metrics or kwargs.get("use_v0"):
                view._write(file=tmp_file, use_v0=True)
            else:
                view._write(file=tmp_file)
            tmp_file.flush()
            tmp_file.seek(0)
            # TODO: retry
            return self._whylabs_client.do_upload(
                dataset_timestamp_epoch,
                upload_url,
                profile_id,
                profile_file=tmp_file,
            )

    def _upload_view(
        self,
        view: Union[DatasetProfileView, SegmentedDatasetProfileView],
        profile_id: str,
        upload_url: str,
        dataset_timestamp_epoch: Optional[int] = None,
        whylabs_tags: Optional[List[SegmentTag]] = None,
        **kwargs: Any,
    ) -> Tuple[bool, str]:
        view = self._prepare_view_for_upload(view)
        return self._send_writable_to_whylabs(
            view, profile_id, upload_url, dataset_timestamp_epoch, whylabs_tags, **kwargs
        )

    def _get_view_of_writable(self, file: _Writable) -> Union[DatasetProfileView, SegmentedDatasetProfileView]:
        if isinstance(file, ViewResultSet):
            return file.view()

        if isinstance(file, ProfileResultSet):
            return file.profile().view()

        if isinstance(file, DatasetProfile):
            return file.view()

        if isinstance(file, SegmentedDatasetProfileView):
            return file

        if not isinstance(file, DatasetProfileView):
            raise ValueError(f"Incompatable _Writable type {type(file)}")

        return file
