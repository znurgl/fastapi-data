from typing import List, Union, Tuple

from pydantic import BaseModel


class ReduceRequest(BaseModel):
    text: str


class WordCount(BaseModel):
    wc: List[Tuple[str, int]]



