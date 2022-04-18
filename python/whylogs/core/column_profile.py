import logging
from typing import Any, Dict, List

from .preprocessing import PreprocessColumn
from .projectors import SingleFieldProjector
from .proto import ColumnMessage, MetricComponentMessage
from .schema import ColumnSchema
from .view import ColumnProfileView

logger = logging.getLogger(__name__)


class ColumnProfile(object):
    def __init__(self, name: str, schema: ColumnSchema, cache_size: int):
        self._name = name
        self._schema = schema

        self._metrics = schema.get_metrics(name)
        self._projector = SingleFieldProjector(col_name=name)
        self._success_count = 0
        self._failure_count = 0
        logger.debug("Setting cache size for column: %s with size: %s", self._name, cache_size)
        self._cache_size = cache_size
        self._cache: List[Any] = []

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
        ex_col = PreprocessColumn.apply(series)
        for metric in self._metrics.values():
            res = metric.columnar_update(ex_col)
            self._success_count += res.failures
            self._failure_count += res.successes

    def serialize(self) -> ColumnMessage:
        self.flush()
        components: Dict[str, MetricComponentMessage] = {}
        for metric_name, metric in self._metrics.items():
            for c_name, msg in metric.serialize().metric_components.items():
                components[f"{metric_name}/{c_name}"] = msg
        return ColumnMessage(metric_components=components)

    def view(self) -> ColumnProfileView:
        return ColumnProfileView(
            metrics=self._metrics.copy(),
            success_count=self._success_count,
            failure_count=self._failure_count,
        )
