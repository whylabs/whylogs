from dataclasses import dataclass
from logging import getLogger
from typing import Callable, Dict, Generic, List, Optional, Tuple, TypeVar, Union

from whylogs.core.metrics.metrics import DistributionMetric, Metric
from whylogs.core.view.column_profile_view import ColumnProfileView
from whylogs.core.view.dataset_profile_view import DatasetProfileView

T = TypeVar("T")

logger = getLogger(__name__)

class Interval(Generic[T]):
    """Defines intervals for numeric contraints."""

    lower_bound: Optional[T] = None
    upper_bound: Optional[T] = None

    def check_interval(self, value: T) -> bool:
        valid = True
        if self.upper_bound is not None:
            valid = value <= self.upper_bound
        if valid and self.lower_bound is not None:
            valid = value >= self.lower_bound
        return valid


@dataclass(frozen=False)
class NumericConstraint:
    max: Interval = Interval()
    min: Interval = Interval()

    def validate(self, distribution: DistributionMetric) -> bool:
        return self.min.check_interval(distribution.min) and self.max.check_interval(
            distribution.max
        )

@dataclass
class MetricsSelector:
    metric_name: str
    column_name: Optional[str] = None # <- TODO: return a collection of columns

    def column_profile(self, profile: DatasetProfileView) -> Optional[ColumnProfileView]:
        column_profile_view = profile.get_column(self.column_name)
        if column_profile_view is None:
            logger.warning(f"No column name {self.column_name} found in profile, available columns are: {profile.get_columns()}")
        return column_profile_view

    def apply(self, profile: DatasetProfileView) -> Optional[Metric]:
        column_profile_view = self.column_profile(profile)
        if column_profile_view is None:
            return None
        metric = column_profile_view.get_metric(self.metric_name)
        if metric is None:
            logger.warning(f"{self.metric_name} not found in {self.column_name} available metrics are: {column_profile_view.get_metric_component_paths()}")
        return metric


@dataclass
class MetricConstraint:
    condition: Callable[[Metric], bool]
    name: str
    metric_selector: Optional[MetricsSelector] = None
    require_column_existence: bool = True

    def validate(self, dataset_profile: DatasetProfileView) -> bool:
        column_profile = self.metric_selector.column_profile(dataset_profile)
        if column_profile is None:
            if self.require_column_existence:
                raise ValueError(f"Could not get column {self.metric_selector.column_name} from column {dataset_profile.get_columns()}")
            else: 
               logger.info(f"validate could not find column {self.metric_selector.column_name} but require_column_existence is false so returning True")
               return True
        metric = self.metric_selector.apply(dataset_profile)
        if metric is None:
            logger.info(f"validate could not get metric {self.metric_selector.metric_name} from column {self.metric_selector.column_name} so returning False. Available metrics on column are: {column_profile.get_metric_component_paths()}")
            return False
        return self.condition(metric)

# A metric constraint operates on a specific metric,
# and has checks for the contained components. Generally
# callers can pass a function(metric) -> bool

def validate(dataset_profile_view: DatasetProfileView, column_constraints: Dict[str, Dict[str, MetricConstraint]]) -> bool:
    column_names = column_constraints.keys()
    if len(column_names) == 0:
        logger.warning("validate was called with empty set of constraints, returning True!")
        return True

    for column_name in column_names:
        columnar_constraints = column_constraints[column_name]
        for constraint_name, metric_constraint in columnar_constraints.items():
            result = metric_constraint.validate(dataset_profile_view)
            if not result:
                logger.info(f"{constraint_name} failed on column {column_name}")
                return False
    return True

def report(dataset_profile_view: DatasetProfileView, column_constraints: Dict[str, Dict[str, MetricConstraint]]) -> List[Tuple[str, int, int]]:
    column_names = column_constraints.keys()
    results = []
    if len(column_names) == 0:
        logger.warning("report was called with empty set of constraints!")
        return results
    
    for column_name in column_names:
        columnar_constraints = column_constraints[column_name]
        for constraint_name, metric_constraint in columnar_constraints.items():
            result = metric_constraint.validate(dataset_profile_view)
            if not result:
                logger.info(f"{constraint_name} failed on column {column_name}")
                results.append(Tuple(constraint_name, 0, 1))
            else:
                results.append(Tuple(constraint_name, 1, 0))
    return results

class Constraints:
    column_constraints: Dict[str, Dict[str, MetricConstraint]] = dict()
    dataset_profile_view: Optional[DatasetProfileView] = None

    def __init__(self, dataset_profile_view: Optional[DatasetProfileView] = None, column_constraints: Optional[Dict[str, Dict[str, MetricConstraint]]] = None) -> None:
        if column_constraints is None:
            column_constraints = dict()
        self._column_constraints = column_constraints
        self._dataset_profile_view = dataset_profile_view

    def validate(self, profile_view: Optional[DatasetProfileView] = None) -> bool:
        profile = self._resolve_profile_view(profile_view)
        return validate(profile, self._column_constraints)

    def report(self, profile_view: Optional[DatasetProfileView] = None):
        profile = self._resolve_profile_view(profile_view)
        return report(profile, self._column_constraints)

    def _resolve_profile_view(self, profile_view: Optional[DatasetProfileView]) -> DatasetProfileView:
        if profile_view is None and self._dataset_profile_view is None:
            raise ValueError("Constraints need to be initialized on a profile or have a profile passed in before calling validate or report.")
        return profile_view if profile_view is not None else self._dataset_profile_view


class ConstraintsBuilder:
    def __init__(self, dataset_profile_view: DatasetProfileView, constraints: Optional[Constraints] = None) -> None:
        self._dataset_profile_view = dataset_profile_view
        if constraints is None:
            constraints = Constraints(dataset_profile_view=dataset_profile_view)
        self._constraints = constraints

    def add_constraint(self, column_name: str,
        constraint: Optional[MetricConstraint] = None,
        max_less_than: Optional[Union[int,float]] = None,
        min_greater_than: Optional[Union[int,float]] = None,
        count_greater_than: Optional[Union[int,float]] = None,
        count_less_than: Optional[Union[int,float]] = None,
        unique_ratio_greater_than: Optional[float] = None,
        unique_ratio_less_than: Optional[float] = None,
        estimated_unique_values_greater_than: Optional[int] = None,
        estimated_unique_values_less_than: Optional[int] = None,
        contains_integers: Optional[bool] = None,
        contains_fractions: Optional[bool] = None,
        contains_strings: Optional[bool] = None,
        contains_objects: Optional[bool] = None,
        contains_nulls: Optional[bool] = None,
        contains_frequent_item: Optional[Union[int,float,str]] = None
        ):
        # check that the column exists and we have a column profile view
        column_profile_view = self._dataset_profile_view.get_column(column_name)
        if column_profile_view is None:
            raise ValueError(f"{column_name} was not found in set of this profile, the lis of columns are: {self._dataset_profile_view.get_columns()}")

        if max_less_than is not None:
            metric_selector = MetricsSelector(column_name = column_name, metric_name = "distribution")
            distribution_metric = metric_selector.apply(self._dataset_profile_view)
            if distribution_metric is None:
                raise ValueError(f"max_less_than could not be applied to {column_name} because it requires a distribution metric, available metrics are: {column_profile_view.get_metric_component_paths()}")
            self._build_max_less_than_constraints(metric_selector, max_less_than, self._constraints.column_constraints)
        #TODO: implement the rest

        return self


    def _build_max_less_than_constraints(self, selector: MetricsSelector, max_less_than: Union[int,float], constraints: Dict[str, Dict[str, MetricConstraint]]) -> MetricConstraint:
        def distribution_condition(distribution_metric) -> bool:
            return distribution_metric.max < max_less_than
        column_name = selector.column_name
        constraint_name = f"max_less_than {max_less_than}"
        metric_constraint = MetricConstraint(condition=distribution_condition, metric_selector = selector, name = constraint_name)

        if column_name not in constraints:
            constraints[column_name] = dict()
        column_constraints = constraints[column_name]
        column_constraints[metric_constraint.name] = metric_constraint
        return metric_constraint

    def execute(self) -> Constraints:
        return self._constraints

