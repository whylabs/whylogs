import logging
from typing import Any, Dict, List

from whylogs.core.metrics import Metric
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.projectors import FieldProjector
from whylogs.core.proto import ColumnMessage
from whylogs.core.schema import ColumnSchema
from whylogs.core.view import ColumnProfileView

logger = logging.getLogger(__name__)


class ColumnProfile(object):
    def __init__(self, name: str, schema: ColumnSchema, cache_size: int):
        self._name = name
        self._schema = schema

        self._metrics = schema.get_metrics(name)
        self._column_validators = schema.get_validators(name)
        self._projector = FieldProjector(col_names=[name])
        self._success_count = 0
        self._failure_count = 0
        logger.debug("Setting cache size for column: %s with size: %s", self._name, cache_size)
        self._cache_size = cache_size
        self._cache: List[Any] = []

    def add_metric(self, metric: Metric) -> None:
        if metric.namespace in self._metrics:
            logger.warning(f"Replacing metric {metric.namespace} in column {self._name}")

        self._metrics[metric.namespace] = metric

    def track(self, row: Dict[str, Any]) -> None:
        value = self._projector.apply(row)
        if len(self._cache) < self._cache_size - 1:
            self._cache.append(value)
        else:
            self._cache.append(value)
            cache_list = self._cache
            self._cache = []
            self.track_column(cache_list)

    def flush(self) -> None:
        """Force emptying the cache and update the internal metrics."""

        logger.debug("Flushing out the cache for col: %s. Cache size: %s", self._name, self._cache_size)
        old_cache = self._cache
        self._cache = []
        self.track_column(old_cache)

    def track_column(self, series: Any) -> None:
        ex_col = PreprocessedColumn.apply(series)
        for metric in self._metrics.values():
            res = metric.columnar_update(ex_col)
            self._failure_count += res.failures
            self._success_count += res.successes
        for validator in self._column_validators:
            validator.columnar_validate(ex_col)

    def to_protobuf(self) -> ColumnMessage:
        self.flush()

        return self.view().to_protobuf()

    def view(self) -> ColumnProfileView:
        self.flush()

        return ColumnProfileView(
            metrics=self._metrics.copy(),
            success_count=self._success_count,
            failure_count=self._failure_count,
        )
