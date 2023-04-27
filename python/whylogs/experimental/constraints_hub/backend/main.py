import uvicorn
from fastapi import FastAPI

from whylogs.experimental.constraints_hub.backend.routers import router

app = FastAPI()
app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(app, port=8000)
