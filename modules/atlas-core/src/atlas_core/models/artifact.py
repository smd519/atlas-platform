from pydantic import BaseModel


class Artifact(BaseModel):
    id: str
    dataset_id: str
    path: str
    format: str
