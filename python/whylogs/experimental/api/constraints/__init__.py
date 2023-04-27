from typing_extensions import TypedDict
from typing import Optional, Union, List
from whylogs.core.constraints.factories import no_missing_values
from whylogs.core.constraints.factories import is_in_range
from whylogs.core.constraints.factories import column_is_probably_unique
from logging import getLogger
from whylogs.core.constraints.metric_constraints import Constraints, MetricConstraint, DatasetConstraint
import yaml

logger = getLogger(__name__)


constraints_mapping = {
    "no_missing_values": no_missing_values,
    "is_in_range": is_in_range,
    "column_is_probably_unique": column_is_probably_unique,
}


def assemble_constraint(constraint_dict: dict) -> Optional[TypedDict]:
    constraint_name = None
    if constraint_dict.get("factory") in constraints_mapping:
        constraint_function = constraints_mapping[constraint_dict.get("factory")]
        constraint_dict.pop("factory")
        constraint_dict.pop("metric")
        if constraint_dict.get("name"):
            constraint_name = constraint_dict.get("name")
            constraint_dict.pop("name")
        returned_constraint = constraint_function(**constraint_dict)
        # todo: if name from yaml is different from the default name of factory, we should retain the name from yaml
        return returned_constraint
    else:
        logger.warning(f"Constraint factory {constraint_dict.get('factory')} not found.")
        return None


def read_constraints_from_yaml(input_path: str) -> List[Union[MetricConstraint, DatasetConstraint]]:
    constraints = []
    with open(input_path, "r") as f:
        data = yaml.safe_load(f)
    checks = data["constraints"]
    for check in checks:
        params = {k: v for k, v in check.items()}
        constraints.append(assemble_constraint(params))
    if constraints:
        return constraints
    else:
        logger.warning(f"No constraints found in {input_path}.")
        return None


def write_constraints_to_yaml(constraints: Constraints, output_path: str):
    constraints_list = []
    for column_name, column_constraints in constraints.column_constraints.items():
        for constraint_name, constraint in column_constraints.items():
            if constraint._params:
                constraints_list.append(constraint._params)
            else:
                logger.warning(
                    f"Constraint {constraint_name} for column {column_name} - no parameters found. Skipping."
                )
    for dataset_constraint in constraints.dataset_constraints:
        if dataset_constraint._params:
            constraints_list.append(dataset_constraint._params)
        else:
            logger.warning(f"Constraint {constraint_name} - no parameters found. Skipping.")
    if constraints_list:
        with open(output_path, "w") as f:
            yaml.dump({"constraints": constraints_list}, f)
