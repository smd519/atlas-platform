import uuid


def generate_execution_id() -> str:
    return str(uuid.uuid4())


def generate_dataset_id() -> str:
    return str(uuid.uuid4())
