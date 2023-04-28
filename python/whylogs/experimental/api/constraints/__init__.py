from logging import getLogger
from typing import List, Optional, Union

import yaml
from typing_extensions import TypedDict

from whylogs.core.constraints import PrefixCondition
from whylogs.core.constraints.factories import (
    column_is_probably_unique,
    condition_meets,
    count_below_number,
    distinct_number_in_range,
    is_in_range,
    is_non_negative,
    no_missing_values,
)
from whylogs.core.constraints.metric_constraints import (
    Constraints,
    DatasetConstraint,
    MetricConstraint,
)

logger = getLogger(__name__)


constraints_mapping = {
    "no_missing_values": {
        "constraint_function": no_missing_values,
        "whylabs_datatypes": ["string", "integral", "fractional", "bool", "unknown"],
    },
    "is_in_range": {"constraint_function": is_in_range, "whylabs_datatypes": ["integral", "fractional"]},
    "column_is_probably_unique": {
        "constraint_function": column_is_probably_unique,
        "whylabs_datatypes": ["string", "integral"],
    },
    "distinct_number_in_range": {
        "constraint_function": distinct_number_in_range,
        "whylabs_datatypes": ["string", "integral"],
    },
    "count_below_number": {
        "constraint_function": count_below_number,
        "whylabs_datatypes": ["string", "integral", "fractional", "bool", "unknown"],
    },
    "is_non_negative": {"constraint_function": is_non_negative, "whylabs_datatypes": ["integral", "fractional"]},
    "condition_meets": {
        "constraint_function": condition_meets,
        "whylabs_datatypes": ["string", "integral", "fractional", "bool", "unknown"],
    },
}


def assemble_constraint(constraint_dict: dict) -> Optional[TypedDict]:
    constraint_name = None
    if constraint_dict.get("expression") and constraint_dict.get("name"):
        condition = PrefixCondition(constraint_dict.get("expression"))
        returned_constraint = DatasetConstraint(condition=condition, name=constraint_dict.get("name"))
        return returned_constraint
    if constraint_dict.get("factory") in constraints_mapping:
        constraint_function = constraints_mapping[constraint_dict.get("factory")]["constraint_function"]
        constraint_dict.pop("factory")
        constraint_dict.pop("metric")
        if constraint_dict.get("name"):
            constraint_name = constraint_dict.get("name")
            constraint_dict.pop("name")
        returned_constraint = constraint_function(**constraint_dict)
        returned_constraint.name = constraint_name
        return returned_constraint
    else:
        logger.warning(f"Constraint factory {constraint_dict.get('factory')} not found.")
        return None


class ConstraintTranslator:
    def validate_params(self, params: dict):
        if not params.get("factory") and not params.get("expression"):
            raise ValueError("Constraint must have a factory or an expression.")
        if params.get("expression") and not params.get("name"):
            raise ValueError("Constraints with an expression must have a name.")

    def read_constraints_from_yaml(
        self, input_path: Optional[str] = None, input_str: Optional[str] = None
    ) -> List[Union[MetricConstraint, DatasetConstraint]]:
        constraints = []
        if input_path is None and input_str is None:
            raise ValueError("Must provide either input_path or input_str.")
        if input_str is not None:
            data = yaml.safe_load(input_str)
        else:
            with open(input_path, "r") as f:
                data = yaml.safe_load(f)
        checks = data["constraints"]
        for check in checks:
            params = {k: v for k, v in check.items()}
            self.validate_params(params)
            constraints.append(assemble_constraint(params))
        if constraints:
            return constraints
        else:
            logger.warning(f"No constraints found in {input_path}.")
            return None

    def write_constraints_to_yaml(
        self,
        constraints: List[Union[MetricConstraint, DatasetConstraint]],
        output_path: Optional[str] = None,
        output_str: Optional[bool] = False,
        org_id: Optional[str] = None,
        version: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ):
        if output_path is None and output_str is None:
            raise ValueError("Must provide either output_path or output_str.")
        constraints_list = []
        for constraint in constraints:
            constraint_name = constraint.name
            if isinstance(constraint, MetricConstraint):
                if constraint._params and constraint._params.get("factory"):
                    constraint_params = constraint._params
                    try:
                        constraint_params["metric"] = constraint.metric_selector.metric_name
                    except:
                        raise ValueError(f"Metric selector not found for constraint {constraint_name}.")
                    constraint_params["name"] = constraint_name
                    constraint_params["column_name"] = constraint.metric_selector.column_name
                    constraints_list.append(constraint_params)
                else:
                    logger.warning(f"Constraint {constraint_name} - no parameters found. Skipping.")

            elif isinstance(constraint, DatasetConstraint):
                constraint_params = {"metric": "dataset-metric"}
                if constraint.condition and isinstance(constraint.condition, PrefixCondition):
                    constraint_params["name"] = constraint.name
                    constraint_params["expression"] = constraint.condition._expression
                    constraints_list.append(constraint_params)
                else:
                    logger.warning(f"Constraint {constraint_name} - no parameters found. Skipping.")

        to_dump = {"constraints": constraints_list}
        if org_id:
            to_dump["org_id"] = org_id
        if dataset_id:
            to_dump["dataset_id"] = dataset_id
        if version:
            to_dump["version"] = version
        if output_str:
            return yaml.dump(to_dump)
        else:
            with open(output_path, "w") as f:
                yaml.dump(to_dump, f)
