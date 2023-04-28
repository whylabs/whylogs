import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from whylogs.experimental.constraints_hub.backend.routers import router

app = FastAPI()
app.include_router(router)

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    uvicorn.run(app, port=8000)
