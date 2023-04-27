import os

from fastapi import APIRouter

# from .models import Constraints

router = APIRouter()


@router.on_event("startup")
def get_environment_variables():
    global org_id
    try:
        org_id = os.environ["ORG_ID"]
    except ValueError:
        raise Exception("you must define ORG_ID")


@router.get("/entity_schema")
def get_entity_schema() -> None:  # Constraints:
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
    pass


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
