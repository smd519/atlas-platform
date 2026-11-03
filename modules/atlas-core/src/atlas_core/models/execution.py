from pydantic import BaseModel


class Execution(BaseModel):
    id: str
    status: str
