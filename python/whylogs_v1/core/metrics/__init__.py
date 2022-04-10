from .metrics import (
    CustomMetricMixin,
    MergeableMetric,
    OperationResult,
    UpdatableMetric,
)
from .numbers import (
    MergeableCountMetric,
    MergeableDistribution,
    MergeableMaxMetric,
    MergeableMinIntMetric,
    UpdatableCountMetric,
    UpdatableDistribution,
    UpdatableMaxIntMetric,
    UpdatableMinIntMetric,
    UpdatableNullCountMetric,
)
from .registry import get_registry
from .strings import MergeableFrequentItemsMetric, UpdatableFrequentItemsMetric
from .uniqueness import MergeableUniquenessMetric, UpdatableUniquenessMetric

get_registry().register_types("cnt", m_type=MergeableCountMetric, u_type=UpdatableCountMetric)
get_registry().register_types("int.max", m_type=MergeableMaxMetric, u_type=UpdatableMaxIntMetric)
get_registry().register_types("int.min", m_type=MergeableMaxMetric, u_type=UpdatableMaxIntMetric)
get_registry().register_types("null", m_type=MergeableCountMetric, u_type=UpdatableNullCountMetric)
get_registry().register_types("hist", m_type=MergeableDistribution, u_type=UpdatableDistribution)
get_registry().register_types("uniq", m_type=MergeableUniquenessMetric, u_type=UpdatableUniquenessMetric)
get_registry().register_types("fi", m_type=MergeableFrequentItemsMetric, u_type=UpdatableFrequentItemsMetric)

__ALL__ = [
    OperationResult,
    UpdatableMetric,
    MergeableMetric,
    CustomMetricMixin,
    # Numbers
    UpdatableCountMetric,
    UpdatableNullCountMetric,
    UpdatableMinIntMetric,
    UpdatableMaxIntMetric,
    MergeableMinIntMetric,
    MergeableMaxMetric,
    UpdatableDistribution,
    MergeableCountMetric,
    # Strings
    MergeableFrequentItemsMetric,
    UpdatableFrequentItemsMetric,
    MergeableUniquenessMetric,
    UpdatableUniquenessMetric,
    # Registry
    get_registry,
]
