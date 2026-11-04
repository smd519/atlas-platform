from pydantic import BaseModel


class SourceConfig(BaseModel):
    source_id: str
    source_name: str
    source_type: str
    config: dict[str, object] = {}
