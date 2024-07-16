from datetime import datetime
from logging import getLogger
from typing import Any, List, Optional, Tuple, Union

from whylogs.api.writer import WriterWrapper, _Writable
from whylogs.core.utils import deprecated

logger = getLogger(__name__)


class EstimationResult(_Writable):
    """
    The result of a performance estimation.
    accuracy: The estimated accuracy.
    reference_partition_id: The partition id of the reference result set.
    reference_result_timestamp: The timestamp of the reference result set.
    """

    def __init__(
        self,
        accuracy: float,
        reference_partition_id: Optional[str] = None,
        reference_result_timestamp: Optional[datetime] = None,
        target_result_timestamp: Optional[datetime] = None,
    ):
        self.accuracy = accuracy
        self.reference_partition_id = reference_partition_id
        self.reference_result_timestamp = reference_result_timestamp
        self.target_result_timestamp = target_result_timestamp

    def _get_default_filename(self) -> str:
        raise ValueError("I'm not a real Writable")

    def _get_default_path(self) -> str:
        raise ValueError("I'm not a real Writable")

    @deprecated(message="please use a Writer")
    def write(self, path: Optional[str] = None, **kwargs: Any) -> Tuple[bool, str]:
        raise ValueError("I'm not a real Writable")

    def _write(self, path: Optional[str] = None, **kwargs: Any) -> Tuple[bool, Union[str, List[str]]]:
        raise ValueError("I'm not a real Writable")


EstimationResultWriter = WriterWrapper  # only works with WhylabsWriter
