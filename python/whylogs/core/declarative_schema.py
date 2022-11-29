from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, TypeVar
import logging

from whylogs.core.datatypes import DataType, TypeMapper
from whylogs.core.metrics.metrics import Metric, MetricConfig
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import DatasetSchema, ColumnSchema
from whylogs.core.segmentation_partition import SegmentationPartition

logger = logging.getLogger(__name__)


@dataclass
class MetricSpec:
    metric: Any  # Should be a Metric class
    config: Optional[MetricConfig] = None


@dataclass
class ResolverSpec:
    column_name: Optional[str] = None  # TODO: maybe make this a regex
    column_type: Optional[Any] = None  # TODO: must be <: DataType
    metrics: List[MetricSpec] = field(default_factory=list)


class DeclarativeResolver(Resolver):

    def __init__(
            self,
            resolvers: List[ResolverSpec],
            default_config: Optional[MetricConfig] = None
    ) -> None:
        # Validate resolvers -- must have name xor type, MetricSpec metrcis must <: Metric
        for spec in resolvers:
            if spec.column_name and spec.column_type:
                logger.warning(f"DeclarativeSchema: column {spec.column_name} also specified type, name takes precedence")
            if not (spec.column_name or spec.column_type):
                raise ValueError("DeclarativeSchema: resolver specification must supply name or type")

            if spec.column_type and not issubclass(spec.column_type, DataType):
                raise ValueError("DeclarativeSchema: resolver specification column type must be a DataType")

            for metric_spec in spec.metrics:
                if not issubclass(metric_spec.metric, Metric):
                    raise ValueError("DeclarativeSchema: must supply a Metric subclass to MetricSpec")
            # TODO: should we allow metric list to be empty?

        self._resolvers = resolvers

    def resolve(
            self,
            name: str,
            why_type: DataType,
            column_schema: ColumnSchema
    ) -> Dict[str, Metric]:
        result: Dict[str, Metric] = {}
        for resolver_spec in self._resolvers:
            col_name, col_type = resolver_spec.column_name, resolver_spec.column_type
            if (col_name and col_name == name) or (col_name is None and isinstance(why_type, col_type)):
                for spec in resolver_spec.metrics:
                    result[spec.metric.get_namespace()] = spec.metric.zero(spec.config or column_schema.cfg)
        # TODO: complain if result is empty
        return result


class DeclarativeSchema(DatasetSchema):

    def __init__(
        self,
        resolvers: List[ResolverSpec],
        types: Optional[Dict[str, Any]] = None,
        default_config: Optional[MetricConfig] = None,
        type_mapper: Optional[TypeMapper] = None,
        cache_size: int = 1024,
        schema_based_automerge: bool = False,
        segments: Optional[Dict[str, SegmentationPartition]] = None,
    ) -> None:
        if not resolvers:
            logger.error("No columns specified in DeclarativeSchema")
        resolver = DeclarativeResolver(resolvers, default_config)
        super().__init__(
            types = types,
            default_configs = default_config,
            type_mapper = type_mapper,
            resolvers = resolver,
            cache_size = cache_size,
            schema_based_automerge = schema_based_automerge,
            segments = segments
        )
