from pydantic import BaseModel


class Dataset(BaseModel):
    id: str
    name: str
