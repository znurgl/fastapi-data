from fastapi import FastAPI
from . import schemas, service


reducer_api = FastAPI()


@reducer_api.post("/reduce", response_model=schemas.WordCount)
def create_user(text: schemas.ReduceRequest):
    return service.reduce(text)
