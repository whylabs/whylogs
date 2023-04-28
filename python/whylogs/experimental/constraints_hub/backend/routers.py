import os

from fastapi import APIRouter, Body
from whylogs.experimental.constraints_hub.backend.models import EntitySchema, ConstraintsPerDatatype, YamlConstraint
import whylabs_client
from whylabs_client.api import models_api
from whylogs.experimental.api.constraints import constraints_mapping
from whylogs.core.constraints.factories import no_missing_values
from whylogs.experimental.api.constraints import ConstraintTranslator
from semantic_version import Version
from whylogs.experimental.extras.confighub import LocalGitConfigStore
from semantic_version import Version
from typing_extensions import Annotated

# from .models import Constraints

router = APIRouter()

original_constraints = [no_missing_values("legs")]
updated_constraints = original_constraints.copy()


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
    return {"constraints_yaml": str(latest_yaml)}


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


@router.post("/save")
def save_constraint_to_file() -> None:
    yaml_string = translator.write_constraints_to_yaml(
        constraints=updated_constraints, output_str=True, org_id=org_id, dataset_id=dataset_id
    )

    # salvar as alterações no YAML construido
    return {"yaml_string": yaml_string}


yaml_example = """

constraints:
- column_name: weight
  factory: no_missing_values
  metric: counts
  name: customname

    """


@router.post("/push_constraints")
def push_constraints(
    yaml_constraint: Annotated[YamlConstraint, Body(example={"constraints_yaml": yaml_example})]
) -> None:
    yaml_string = yaml_constraint.constraints_yaml
    constraints = translator.read_constraints_from_yaml(input_str=yaml_string)
    cur_ver = cs.get_version_of_latest()
    new_ver = cur_ver.next_major()
    content = translator.write_constraints_to_yaml(
        constraints=constraints, org_id=org_id, dataset_id=dataset_id, output_str=True, version=str(new_ver)
    )
    cs.propose_version(content, new_ver, "testing new version")
    cs.commit_version(new_ver)
    return {"version": str(new_ver)}
