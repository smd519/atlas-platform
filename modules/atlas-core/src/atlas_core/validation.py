def require_not_empty(value: str, field_name: str) -> None:
    if not value:
        raise ValueError(f"{field_name} cannot be empty")
