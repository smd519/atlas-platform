from pydantic import BaseModel


class StorageResult(BaseModel):
    success: bool
    path: str
    row_count: int | None = None
    message: str | None = None