from datetime import datetime
from logging import getLogger
from typing import Any, Optional

from whylogs.api.writer import Writer, Writers

logger = getLogger(__name__)


class EstimationResult:
    """
    The result of a performance estimation.
    accuracy: The estimated accuracy.
    reference_partition_id: The partition id of the reference result set.
    reference_result_timestamp: The timestamp of the reference result set.
    """

    def __init__(
        self,
        accuracy: Optional[float] = None,
        reference_partition_id: Optional[str] = None,
        reference_result_timestamp: Optional[datetime] = None,
        target_result_timestamp: Optional[datetime] = None,
    ):
        self.accuracy = accuracy
        self.reference_partition_id = reference_partition_id
        self.reference_result_timestamp = reference_result_timestamp
        self.target_result_timestamp = target_result_timestamp

    def writer(self, name: str = "whylabs") -> "EstimationResultWriter":
        if name != "whylabs":
            raise ValueError("Only whylabs writer is currently supported")
        writer = Writers.get(name)
        return EstimationResultWriter(results=self, writer=writer)


class EstimationResultWriter:
    def __init__(self, results: EstimationResult, writer: Writer):
        self._estimation_result = results
        self._writer = writer

    def write(self, **kwargs: Any) -> None:
        self._writer.write(file=self._estimation_result, **kwargs)
        logger.debug("Completed writing estimation result!")
