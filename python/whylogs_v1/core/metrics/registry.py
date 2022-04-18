# from typing import Callable, Dict, Type, TypeVar
#
# UM_LAMBDA = Callable[[], UpdatableMetric]
# MM_LAMBDA = Callable[[], MergeableMetric]
#
# UM = TypeVar("UM", bound=UpdatableMetric)
# MM = TypeVar("MM", bound=MergeableMetric)
#
#
# class _MetricRegistry(object):
#     _updatable_metrics: Dict[str, UM_LAMBDA] = {}
#     _mergeable_metrics: Dict[str, MM_LAMBDA] = {}
#
#     def get_updatable_metric(self, m_name: str) -> UpdatableMetric:
#         return self._updatable_metrics[m_name]()
#
#     def get_mergeable_metric(self, m_name: str) -> MergeableMetric:
#         return self._mergeable_metrics[m_name]()
#
#     def register_mergeable(self, name: str, func: MM_LAMBDA) -> None:
#         if self._updatable_metrics.get(name) is not None:
#             raise ValueError(f"Mergeable metric already registered for {name}")
#         self._mergeable_metrics[name] = func
#
#     def register_updatable(self, name: str, func: UM_LAMBDA) -> None:
#         if self._updatable_metrics.get(name) is not None:
#             raise ValueError(f"Updatable metric already registered for {name}")
#         self._updatable_metrics[name] = func
#
#     def register(self, name: str, m_func: MM_LAMBDA, u_func: UM_LAMBDA) -> None:
#         self.register_mergeable(name, m_func)
#         self.register_updatable(name, u_func)
#
#     def register_types(self, name: str, u_type: Type[UM], m_type: Type[MM]) -> None:
#         self.register_mergeable(name, lambda: m_type())  # type: ignore
#         self.register_updatable(name, lambda: u_type())  # type: ignore
#
#
# _INSTANCE = _MetricRegistry()
#
#
# def get_registry() -> _MetricRegistry:
#     return _INSTANCE
