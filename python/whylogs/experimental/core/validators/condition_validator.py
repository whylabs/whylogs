from copy import copy
from typing import Any, Callable, List, Optional, Union

from whylogs.core.validators import ConditionValidator
from whylogs.experimental.core.validators.validator import append_validator


def condition_validator(
    col_names: Union[str, List[str]],
    condition_name: Optional[str] = None,
    actions: Union[
        List[Callable[[str, str, Any, Optional[Any]], None]], Callable[[str, str, Any, Optional[Any]], None]
    ] = [],
    namespace: Optional[str] = None,
    schema_name: str = "",
    enable_sampling: bool = True,
) -> Callable[[Any], Any]:
    col_names = col_names if isinstance(col_names, list) else [col_names]
    actions = actions if isinstance(actions, list) else [actions]

    def decorator_register(func):
        name = condition_name or func.__name__
        name = f"{namespace}.{name}" if namespace else name
        for col in col_names:
            validator = ConditionValidator(
                name=name,
                conditions={name: func},
                actions=copy(actions),
                enable_sampling=enable_sampling,
            )
            append_validator(schema_name, col, validator)
        return func

    return decorator_register
