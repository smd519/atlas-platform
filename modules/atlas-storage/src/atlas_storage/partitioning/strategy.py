from datetime import datetime


class PartitionStrategy:
    @staticmethod
    def by_date(
        execution_date: datetime,
    ) -> str:
        return (
            f"year={execution_date:%Y}/"
            f"month={execution_date:%m}/"
            f"day={execution_date:%d}"
        )