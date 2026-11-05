from pydantic import BaseModel


class QualityResult(BaseModel):
    passed: bool
    rule_name: str
    message: str | None = None
