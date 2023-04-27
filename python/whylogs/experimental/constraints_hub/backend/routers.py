import os

from fastapi import APIRouter
from whylogs.experimental.constraints_hub.backend.models import EntitySchema
import whylabs_client
from whylabs_client.api import models_api

# from .models import Constraints

router = APIRouter()


@router.on_event("startup")
def get_environment_variables():
    global org_id
    global dataset_id
    global api_endpoint
    global api_key
    try:
        org_id = os.environ["WHYLABS_DEFAULT_ORG_ID"]
        dataset_id = os.environ["WHYLABS_DEFAULT_DATASET_ID"]
        api_endpoint = os.environ["WHYLABS_API_ENDPOINT"]
        api_key = os.environ["WHYLABS_API_KEY"]
    except KeyError:
        raise Exception(
            "you must define WHYLABS_DEFAULT_ORG_ID, WHYLABS_DEFAULT_DATASET_ID and WHYLABS_API_KEY environment variables"
        )


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
        columns_entity = {}
        for column_name, column_dict in api_response["columns"].items():
            if column_name[0] != "Ω":
                column_info = {
                    "classifier": column_dict.classifier,
                    "data_type": column_dict.data_type,
                    "discreteness": column_dict.discreteness,
                }
                column_scheme = {column_name: column_info}
                columns_entity.update(column_scheme)
        return {"columns": columns_entity}


@router.get("/types_to_constraints")
def get_column_types_to_constraints() -> None:
    """
    {
        float: [cons1, cons2, cons3],
        int: [cons4],
        ...
    }
    """
    pass


@router.post("/save")
def save_constraint_to_file() -> None:
    # salvar as alterações no YAML construido
    pass
