from collections import defaultdict
from copy import copy
from typing import Any, Callable, Dict, List, Optional, Union

from whylogs.core.validators import ConditionValidator

_validator_udfs: Dict[str, List[Dict[str, List[ConditionValidator]]]] = defaultdict(list)


def condition_validator(
    col_names: Union[str, List[str]],
    condition_name: Optional[str] = None,
    actions: List[Callable[[str, str, Any, Optional[Any]], None]] = [],
    namespace: Optional[str] = None,
    schema_name: str = "",
    enable_sampling: bool = True,
) -> Callable[[Any], Any]:
    col_names = col_names if isinstance(col_names, list) else [col_names]

    def decorator_register(func):
        global _validator_udfs
        name = condition_name or func.__name__
        name = f"{namespace}.{name}" if namespace else name
        for col in col_names:
            validator = ConditionValidator(
                name=name,
                conditions={name: func},
                actions=copy(actions),
                enable_sampling=enable_sampling,
            )
            _validator_udfs[schema_name].append({col: [validator]})
        return func

    return decorator_register


def generate_validators(
    initial_validators: Optional[Dict[str, List[ConditionValidator]]],
    schema_name: Union[str, List[str]],
    include_default_schema: bool = True,
) -> Dict[str, List[ConditionValidator]]:
    """Merge registered validators for requested schemas"""
    global _validator_udfs
    schema_names = schema_name if isinstance(schema_name, list) else [schema_name]
    if include_default_schema and "" not in schema_names:
        schema_names = [""] + schema_names
    result: Dict[str, List[ConditionValidator]] = defaultdict(list)
    if initial_validators is not None:
        for column, validators in initial_validators.items():
            result[column] += validators
    for name in schema_names:
        for registration in _validator_udfs[name]:
            for column, validators in registration.items():
                result[column] += validators
    return result
