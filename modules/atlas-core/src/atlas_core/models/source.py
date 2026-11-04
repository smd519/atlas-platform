from pydantic import BaseModel


class Source(BaseModel):
    id: str
    name: str
    source_type: str
