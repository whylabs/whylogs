from fastapi import APIRouter

# from .models import Constraints

router = APIRouter()


@router.get("/constraints")
def get_constraints() -> None:  # Constraints:
    pass
