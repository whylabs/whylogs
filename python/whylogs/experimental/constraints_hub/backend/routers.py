import os
import yaml
import json
from fastapi import APIRouter, Body
from whylogs.experimental.constraints_hub.backend.models import EntitySchema, JsonConstraint
import whylabs_client
from whylabs_client.api import models_api
from whylogs.experimental.api.constraints import constraints_mapping
from whylogs.experimental.api.constraints import ConstraintTranslator
from semantic_version import Version
from whylogs.experimental.extras.confighub import LocalGitConfigStore
from semantic_version import Version
from typing_extensions import Annotated

# from .models import Constraints
router = APIRouter()


@router.on_event("startup")
def get_environment_variables():
    global org_id
    global dataset_id
    global api_endpoint
    global api_key
    global cs
    global translator
    try:
        org_id = os.environ["WHYLABS_DEFAULT_ORG_ID"]
        dataset_id = os.environ["WHYLABS_DEFAULT_DATASET_ID"]
        api_endpoint = os.environ["WHYLABS_API_ENDPOINT"]
        api_key = os.environ["WHYLABS_API_KEY"]
    except KeyError:
        raise Exception(
            "you must define WHYLABS_DEFAULT_ORG_ID, WHYLABS_DEFAULT_DATASET_ID and WHYLABS_API_KEY environment variables"
        )
    translator = ConstraintTranslator()
    storage_folder_name = "constraints_storage"
    local_storage_folder = os.path.join(os.getcwd(), storage_folder_name)
    cs = LocalGitConfigStore(org_id, dataset_id, "constraints", repo_path=local_storage_folder)
    cs.create()
    cur_ver = cs.get_version_of_latest()
    if cur_ver == Version("0.0.0"):
        new_ver = cur_ver.next_major()
        content = translator.write_constraints_to_yaml(
            constraints=[], org_id=org_id, dataset_id=dataset_id, output_str=True, version=str(new_ver)
        )
        cs.propose_version(content, new_ver, "testing new version")
        cs.commit_version(new_ver)


@router.get("/entity_schema")
def get_entity_schema() -> EntitySchema:  # Constraints:
    """
    [
        {
            col_name: "my_col",
            col_type: int
        },
        {
            col_name: "my_other",
            col_type: float
        }
    ]
    """
    configuration = whylabs_client.Configuration(host=api_endpoint)
    configuration.api_key["ApiKeyAuth"] = api_key
    configuration.discard_unknown_keys = True
    with whylabs_client.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = models_api.ModelsApi(api_client)
        # example passing only required values which don't have defaults set
        try:
            # Get a list of all of the model ids for an organization.
            api_response = api_instance.get_entity_schema(org_id=org_id, dataset_id=dataset_id)
        except whylabs_client.ApiException as e:
            print("Exception when calling ModelsApi->list_models: %s\n" % e)
            return e
        column_list = []
        for column_name, column_dict in api_response["columns"].items():
            if column_name[0] != "Ω":
                column_scheme = {"column_name": column_name}
                column_scheme.update(
                    {
                        "classifier": column_dict.classifier,
                        "data_type": column_dict.data_type,
                        "discreteness": column_dict.discreteness,
                    }
                )
                column_list.append(column_scheme)
        return {"entity_schema": column_list}


@router.get("/latest_version")
def get_latest_version() -> None:
    latest_yaml = cs.get_latest()
    yaml_data = yaml.load(latest_yaml, Loader=yaml.FullLoader)
    return {"constraints_json": json.dumps(yaml_data)}


@router.get("/types_to_constraints")
def get_column_types_to_constraints() -> None:
    """
    {
        float: [cons1, cons2, cons3],
        int: [cons4],
        ...
    }
    """
    whylabs_datatypes = ["integral", "fractional", "bool", "string", "unknown", "null"]
    constraints_per_type = {}
    for datatype in whylabs_datatypes:
        constraints_list = []
        for constraint_name, constraint in constraints_mapping.items():
            if datatype in constraint["whylabs_datatypes"]:
                constraints_list.append(constraint_name)
        constraints_per_type[datatype] = constraints_list
    return {"constraints_per_datatype": constraints_per_type}


yaml_example = """

constraints:
- column_name: weight
  factory: no_missing_values
  metric: counts
  name: customname

    """

constraint_example = """
{"constraints": [{"column_name": "weight", "factory": "no_missing_values", "metric": "counts", "name": "customname"}]}
"""


@router.post("/push_constraints")
def push_constraints(
    json_constraint: Annotated[JsonConstraint, Body(example={"constraints_json": constraint_example})]
) -> None:
    json_string = json_constraint.constraints_json
    json_data = json.loads(json_string)
    yaml_string = yaml.dump(json_data)

    constraints = translator.read_constraints_from_yaml(input_str=yaml_string)
    cur_ver = cs.get_version_of_latest()
    new_ver = cur_ver.next_major()
    content = translator.write_constraints_to_yaml(
        constraints=constraints, org_id=org_id, dataset_id=dataset_id, output_str=True, version=str(new_ver)
    )
    cs.propose_version(content, new_ver, "testing new version")
    cs.commit_version(new_ver)
    return {"version": str(new_ver)}


@router.get("/get_params/")
async def get_params(my_string: str) -> None:
    """For the moment, it works with: no_missing_values, is_in_range, column_is_probably_unique,
    distinct_number_in_range, count_below_number, is_non_negative, condition_meets"""
    constraint_info = constraints_mapping.get(my_string)
    if constraint_info:
        parameters_class = constraint_info.get("parameters")
        field_types = {k: v.__name__ for k, v in parameters_class.__annotations__.items()}
        field_types.pop("factory")
        return {"parameters": field_types}
    return {}
