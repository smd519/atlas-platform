from datetime import datetime

from pydantic import BaseModel


class Execution(BaseModel):
    id: str
    started_at: datetime
    status: str