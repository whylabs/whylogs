import logging
from typing import Any, List, Optional, Tuple, Union

from whylabs_client import ApiClient

from whylogs.api.whylabs.session.session_manager import INIT_DOCS
from whylogs.api.writer.whylabs_base import WhyLabsWriterBase
from whylogs.api.writer.whylabs_batch_writer import WhyLabsBatchWriter
from whylogs.api.writer.whylabs_client import WhyLabsClient
from whylogs.api.writer.whylabs_estimation_result_writer import (
    WhyLabsEstimationResultWriter,
)
from whylogs.api.writer.whylabs_reference_writer import WhyLabsReferenceWriter
from whylogs.api.writer.whylabs_transaction_writer import WhyLabsTransactionWriter
from whylogs.api.writer.writer import _Writable
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.utils import deprecated, deprecated_alias
from whylogs.experimental.performance_estimation.estimation_results import (
    EstimationResult,
)

logger = logging.getLogger(__name__)


class WhyLabsWriter(WhyLabsWriterBase):
    f"""
    A WhyLogs writer to upload profiles and estimation results onto the WhyLabs platform.

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
    import whylogs as why

    why.init()

    profile = why.log(pandas=df)
    profile.writer("whylabs").write()
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

    @deprecated(message="please use WhyLabsClient instead")
    def tag_output_columns(self, columns: List[str]) -> Tuple[bool, str]:
        """Sets the list of columns as output columns for the specified dataset.

        Parameters
        ----------
        columns : List[str]
            The list of column names you want to tag as outputs.

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean indicating success or failure: e.g. (True, "column prediction was updated to
            output") and string with status message.
        """

        return self._whylabs_client.tag_output_columns(columns)  # type: ignore

    @deprecated(message="please use WhyLabsClient instead")
    def tag_input_columns(self, columns: List[str]) -> Tuple[bool, str]:
        """Sets the list of columns as input columns for the specified dataset.

        Parameters
        ----------
        columns : List[str]
            The list of column names you want to tag as inputs.

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean indicating success or failure: e.g. (True, "column [output_voltage] updated to
            input") and string with status message.
        """

        return self._whylabs_client.tag_input_columns(columns)  # type: ignore

    @deprecated(message="please use WhyLabsClient instead")
    def tag_custom_performance_column(
        self, column: str, label: Optional[str] = None, default_metric: str = "mean"
    ) -> Tuple[bool, str]:
        """Sets the column as a custom performance metric for the specified dataset and org id.
        The default metric will be displayed in the Performance tab in WhyLabs.

        Parameters
        ----------
        column : str
            The column name in the whylogs profile you want to tag as a custom performance metric.
        label : str
            The label that will be displayed in WhyLabs UI. If none is passed, defaults to column name.
        default_metric : str
            The default metric that will be displayed in the Performance tab in WhyLabs.
            For example, "mean", "median", "max", or "min".
            If none is passed, defaults to "mean".

        Note: the resulting custom performance metric is considered an unmergeable metric.

        """
        return self._whylabs_client.tag_custom_performance_column(column, label, default_metric)  # type: ignore

    @deprecated(message="please use WhyLabsWriter::write() instead")
    def write_estimation_result(self, file: EstimationResult, **kwargs: Any) -> Tuple[bool, str]:
        return self.write(file, **kwargs)

    @deprecated(message="please use WhyLabsClient instead")
    def write_feature_weights(self, file: FeatureWeights, **kwargs: Any) -> Tuple[bool, str]:
        """Put feature weights for the specified dataset.

        Parameters
        ----------
        file : FeatureWeights
            FeatureWeights object representing the Feature Weights for the specified dataset

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean (1-success, 0-fail) and string with the request's status code.
        """
        return self.write(file, **kwargs)

    @deprecated(message="please use WhyLabsClient instead")
    def get_feature_weights(self, **kwargs: Any) -> Optional[FeatureWeights]:
        """Get latest version for the feature weights for the specified dataset

        Returns
        -------
        FeatureWeightResponse
            Response of the GET request, with segmentWeights and metadata.
        """
        return self._whylabs_client.get_feature_weights(**kwargs)  # type: ignore

    def start_transaction(self, transaction_id: Optional[str] = None) -> str:
        """
        Initiates a transaction -- any profiles subsequently written by calling write()
        will be uploaded to WhyLabs, but not ingested until commit_transaction() is called. Throws
        on failure.

        Zipped profiles are not supported
        Reference profiles are not supported
        """
        if self._whylabs_client._transaction_id is not None:  # type: ignore
            logger.error("Must end current transaction with commit_transaction() before starting another")
            return self._whylabs_client._transaction_id  # type: ignore

        if transaction_id is not None:
            self._whylabs_client._transaction_id = transaction_id  # type: ignore
            return transaction_id

        # initiate a new transaction
        transaction_id = self._whylabs_client.get_transaction_id()  # type: ignore
        self._whylabs_client._transaction_id = transaction_id  # type: ignore
        return transaction_id

    def commit_transaction(self) -> None:
        """
        Ingest any profiles written since the previous start_transaction().
        Throws on failure.
        """
        if self._whylabs_client._transaction_id is None:  # type: ignore
            logger.error("Must call start_transaction() before commit_transaction()")
            return

        id = self._whylabs_client._transaction_id  # type: ignore
        self._whylabs_client._transaction_id = None  # type: ignore
        self._whylabs_client.commit_transaction(id)  # type: ignore

    def abort_transaction(self) -> None:
        """
        Prevent the profiles uploaded in the current transaction from being
        ingested. You still need to call commit_transaction(), but it will
        throw a NotFoundException to indicate the transaction was unsuccessful.
        """
        self._whylabs_client.abort_transaction(self._whylabs_client._transaction_id)  # type: ignore

    def _get_writer(self, writer_type) -> WhyLabsWriterBase:
        return writer_type(
            self._whylabs_client._org_id,  # type: ignore
            self._whylabs_client._api_key,  # type: ignore
            self._whylabs_client._dataset_id,  # type: ignore
            None,  # self._whylabs_client._api_client,  # TODO: handle custom client
            self._whylabs_client._ssl_ca_cert,  # type: ignore
            self._whylabs_client._timeout_seconds,  # type: ignore
        )

    @deprecated_alias(profile="file")
    def write(
        self, file: _Writable, dest: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        self._whylabs_client = self._whylabs_client.option(**kwargs)  # type: ignore
        if isinstance(file, FeatureWeights):
            # TODO: FeatureWeights should not be a Writable, just use the WL client directly
            return self._whylabs_client.write_feature_weights(file, **kwargs)

        elif isinstance(file, EstimationResult):
            return self._get_writer(WhyLabsEstimationResultWriter).write(file, dest, **kwargs)

        # Writable is some flavor of profile -- DatasetProfile, [Segmented]DatasetProfileView, or ResultSet

        if (self._whylabs_client._transaction_id is not None) or kwargs.get("transaction"):
            if self._whylabs_client._reference_profile_name is not None:
                return False, "Cannot send reference profiles in a transaction"
            return self._get_writer(WhyLabsTransactionWriter).write(
                file, dest, transaction_id=self._whylabs_client._transaction_id, **kwargs
            )

        elif self._whylabs_client._reference_profile_name is not None:
            return self._get_writer(WhyLabsReferenceWriter).write(
                file, dest, reference_profile_name=self._whylabs_client._reference_profile_name, **kwargs
            )

        return self._get_writer(WhyLabsBatchWriter).write(file, dest, **kwargs)


class WhyLabsTransaction:
    def __init__(self, writer: WhyLabsWriter):
        self._writer = writer

    def __enter__(self) -> str:
        if self._writer._whylabs_client._transaction_id is None:
            self._writer.start_transaction()
        return self._writer._whylabs_client._transaction_id  # type: ignore

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        self._writer.commit_transaction()
