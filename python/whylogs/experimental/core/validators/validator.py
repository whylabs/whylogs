from collections import defaultdict
from typing import Dict, List, Optional, Union

from whylogs.core.validators import Validator

_validator_udfs: Dict[str, List[Dict[str, List[Validator]]]] = defaultdict(list)


def append_validator(schema_name, col_name: str, validator: Validator):
    global _validator_udfs
    validator_name = validator.name
    exists = False
    # if validator with same name and column exists, replace it
    for col_validator in _validator_udfs.get(schema_name, []):
        if col_name in col_validator:
            for i, v in enumerate(col_validator[col_name]):
                if v.name == validator_name:
                    exists = True
                    col_validator[col_name][i] = validator
    if not exists:
        _validator_udfs[schema_name].append({col_name: [validator]})


def generate_validators(
    initial_validators: Optional[Dict[str, List[Validator]]],
    schema_name: Union[str, List[str]],
    include_default_schema: bool = True,
) -> Dict[str, List[Validator]]:
    """Merge registered validators for requested schemas"""
    global _validator_udfs
    schema_names = schema_name if isinstance(schema_name, list) else [schema_name]
    if include_default_schema and "" not in schema_names:
        schema_names = [""] + schema_names
    result: Dict[str, List[Validator]] = defaultdict(list)
    if initial_validators is not None:
        for column, validators in initial_validators.items():
            result[column] += validators
    for name in schema_names:
        for registration in _validator_udfs[name]:
            for column, validators in registration.items():
                result[column] += validators
    return result
