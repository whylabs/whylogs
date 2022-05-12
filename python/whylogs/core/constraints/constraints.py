from logging import getLogger
from typing import Any, Callable, List, Mapping, Optional, Tuple, Union

from whylogs.core.proto.v0 import Op
from whylogs.core.utils import get_distribution_metrics
from whylogs.core.view.dataset_profile_view import DatasetProfileView

logger = getLogger(__name__)

_summary_funcs1: Mapping[int, Callable[..., Callable[[Any], Any]]] = {
    # functions that compare a summary field to a literal value.
    Op.LT: lambda f, v: lambda s: getattr(s, f) < v,
    Op.LE: lambda f, v: lambda s: getattr(s, f) <= v,
    Op.EQ: lambda f, v: lambda s: getattr(s, f) == v,
    Op.NE: lambda f, v: lambda s: getattr(s, f) != v,
    Op.GE: lambda f, v: lambda s: getattr(s, f) >= v,
    Op.GT: lambda f, v: lambda s: getattr(s, f) > v,
    Op.BTWN: lambda f, v1, v2: lambda s: v1 <= getattr(s, f) <= v2,
    Op.IN: lambda f, v: lambda s: getattr(s, f) in v,
    Op.CONTAIN: lambda f, v: lambda s: v in getattr(s, f),
}


def _create_column_profile_summary_object(**kwargs: Optional[float]) -> type:
    """
    Wrapper method for summary constraints update object creation

    Parameters
    ----------
    number_summary : NumberSummary
        Summary object generated from NumberTracker
        Used to unpack the metrics as separate items in the dictionary
    kwargs : Summary objects or datasketches objects
        Used to update specific constraints that need additional calculations
    Returns
    -------
    Anonymous object containing all of the metrics as fields with their corresponding values
    """

    column_summary = {}

    column_summary.update(kwargs)

    return type("Object", (), column_summary)


class SummaryConstraint:
    def __init__(
        self,
        first_field: str,
        op: int,
        value: Optional[float] = None,
        second_field: str = None,
        name: str = None,
        verbose: bool = False,
    ) -> None:
        self.first_field = first_field
        self._verbose = verbose
        self._name = name
        self.op = op
        self.second_field = second_field
        self.value = value
        self.total = 0
        self.failures = 0

        if value is not None and second_field is None:
            # field-value summary comparison
            self.func = _summary_funcs1[op](first_field, value)

    def update(self, update_summary: object) -> None:
        constraint_type_str = "table shape" if self.first_field in ("columns", "total_row_number") else "summary"

        self.total += 1

        if not self.func(update_summary):
            self.failures += 1
            if self._verbose:
                logger.info(f"{constraint_type_str} constraint {self._name} failed")

    def report(self) -> Tuple[Any, ...]:
        return (self._name, self.total, self.failures)


class SummaryConstraints:
    def __init__(self, constraints: Union[List[SummaryConstraint], Mapping[str, SummaryConstraint]] = None):
        """
        SummaryConstraints is a container for multiple summary constraints,
        generally associated with a single ColumnProfile.

        Parameters
        ----------
        constraints : Mapping[str, SummaryConstraint]
            A dictionary of summary constraints with their names as keys.
            Can also accept a list of summary constraints.

        """

        if constraints is None:
            constraints = dict()

        # Support list of constraints for back compat with previous version.
        if isinstance(constraints, list):
            self.constraints = {constraint._name: constraint for constraint in constraints}
        elif isinstance(constraints, dict):
            self.constraints = constraints
        else:
            raise ValueError("Unexpected type for constraints parameter.")

    def __getitem__(self, name: str) -> Optional[SummaryConstraint]:
        if self.constraints:
            return self.constraints.get(name)
        return None

    def update(self, v: type) -> None:
        for c in self.constraints.values():
            c.update(v)

    def report(self) -> Optional[List[tuple]]:
        v = [c.report() for c in self.constraints.values()]
        if len(v) > 0:
            return v
        return None


class DatasetConstraints:
    def __init__(
        self,
        summary_constraints: Mapping[str, SummaryConstraints] = None,
    ):
        """
        DatasetConstraints is a container for multiple types of constraint containers, such as ValueConstraints,
        SummaryConstraints, and MultiColumnValueConstraints.
        Used for wrapping constraints that should be applied on a single data set.

        Parameters
        ----------
        summary_constraints : Mapping[str, SummaryConstraints]
            A dictionary where the keys correspond to the name of the feature for which the supplied value
            represents the SummaryConstraints to be executed

        """

        if summary_constraints is None:
            summary_constraints = dict()
        for k, v in summary_constraints.items():
            if isinstance(v, list):
                summary_constraints[k] = SummaryConstraints(v)
        self.summary_constraint_map = summary_constraints

    def __call__(self, profile_view: DatasetProfileView) -> List[Tuple[Any, ...]]:
        return self._constraints_report(profile_view)

    def _constraints_report(self, profile_view: DatasetProfileView) -> List[Tuple[Any, ...]]:
        if not isinstance(profile_view, DatasetProfileView):
            raise ValueError("Constraints must receive a DatasetProfileView object")
        columns = profile_view.get_columns()
        for k, v in self.summary_constraint_map.items():
            if isinstance(v, list):
                self.summary_constraint_map[k] = SummaryConstraints(v)
        for feature_name, constraints in self.summary_constraint_map.items():
            if feature_name in columns:
                min_val, max_val, range_val = get_distribution_metrics(columns[feature_name])
                update_obj = _create_column_profile_summary_object(max=max_val, min=min_val, range=range_val)
                constraints.update(update_obj)
            else:
                logger.debug(f"unkown feature '{feature_name}' in summary constraints")

        return [(k, s.report()) for k, s in self.summary_constraint_map.items()]

    def report(self) -> List[Tuple[str, Optional[List[tuple]]]]:
        l2 = [(k, s.report()) for k, s in self.summary_constraint_map.items()]
        return l2
