from fastapi import FastAPI
from app.reducer.api import reducer_api

app = FastAPI()
app.include_router(reducer_api.router)