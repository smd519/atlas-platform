from datetime import datetime

from pydantic import BaseModel


class ExtractionResult(BaseModel):
    source_id: str
    row_count: int
    extracted_at: datetime
    data: object
