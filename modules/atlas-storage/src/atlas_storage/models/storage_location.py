from pydantic import BaseModel


class StorageLocation(BaseModel):
    layer: str
    path: str
    format: str