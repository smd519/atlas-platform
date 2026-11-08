"""Domain models and cron trigger evaluation for atlas-scheduler."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from atlas_core import require_not_empty
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from atlas_scheduler.exceptions import ScheduleValidationError

_MONTH_NAMES: dict[str, int] = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

_DOW_NAMES: dict[str, int] = {
    "SUN": 0,
    "MON": 1,
    "TUE": 2,
    "WED": 3,
    "THU": 4,
    "FRI": 5,
    "SAT": 6,
}

_MAX_DAYS_PER_MONTH: dict[int, int] = {
    1: 31,
    2: 29,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}


@dataclass(frozen=True)
class ParsedCronExpression:
    """Validated and pre-expanded 5-field cron expression."""

    raw: str
    minutes: tuple[int, ...]
    hours: tuple[int, ...]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    dom_is_wildcard: bool
    dow_is_wildcard: bool

    def matches_date(self, candidate: date) -> bool:
        """Return True if ``candidate`` matches the month, day-of-month, and day-of-week rules."""
        if candidate.month not in self.months:
            return False
        cron_dow = (candidate.weekday() + 1) % 7
        dom_match = candidate.day in self.days_of_month
        dow_match = cron_dow in self.days_of_week
        if not self.dom_is_wildcard and not self.dow_is_wildcard:
            return dom_match or dow_match
        return dom_match and dow_match


def _parse_cron_token(
    token: str,
    min_val: int,
    max_val: int,
    names: dict[str, int] | None = None,
    *,
    is_dow: bool = False,
) -> int:
    clean = token.strip().upper()
    if not clean:
        raise ValueError("Empty cron value token.")
    if names and clean in names:
        return names[clean]
    try:
        val = int(clean)
    except ValueError as exc:
        raise ValueError(f"Invalid cron value '{token}'.") from exc
    if is_dow and val == 7:
        val = 0
    if val < min_val or val > max_val:
        raise ValueError(f"Cron value {val} out of range [{min_val}, {max_val}].")
    return val


def _parse_cron_field(
    field_str: str,
    min_val: int,
    max_val: int,
    *,
    field_name: str,
    names: dict[str, int] | None = None,
    is_dow: bool = False,
) -> tuple[frozenset[int], bool]:
    clean_field = field_str.strip()
    if not clean_field:
        raise ScheduleValidationError(f"Cron field '{field_name}' must not be empty.")

    is_wildcard = clean_field == "*"
    values: set[int] = set()

    for part in clean_field.split(","):
        part = part.strip()
        if not part:
            raise ScheduleValidationError(
                f"Invalid empty list item in cron field '{field_name}': '{field_str}'."
            )

        step = 1
        if "/" in part:
            range_part, step_part = part.split("/", 1)
            try:
                step = int(step_part.strip())
            except ValueError as exc:
                raise ScheduleValidationError(
                    f"Invalid step '{step_part}' in cron field '{field_name}'."
                ) from exc
            if step <= 0:
                raise ScheduleValidationError(
                    f"Cron step in field '{field_name}' must be > 0, got {step}."
                )
        else:
            range_part = part

        if range_part == "*":
            start, end = min_val, max_val
        elif "-" in range_part:
            start_s, end_s = range_part.split("-", 1)
            try:
                start = _parse_cron_token(start_s, min_val, max_val, names, is_dow=is_dow)
                end_raw = end_s.strip().upper()
                if is_dow and end_raw == "7":
                    end = 7
                else:
                    end = _parse_cron_token(end_s, min_val, max_val, names, is_dow=is_dow)
            except ValueError as exc:
                raise ScheduleValidationError(
                    f"Invalid range '{range_part}' in cron field '{field_name}': {exc}"
                ) from exc
            if start > end:
                raise ScheduleValidationError(
                    f"Cron range start ({start}) > end ({end}) in field '{field_name}'."
                )
        else:
            try:
                single = _parse_cron_token(range_part, min_val, max_val, names, is_dow=is_dow)
            except ValueError as exc:
                raise ScheduleValidationError(
                    f"Invalid value '{range_part}' in cron field '{field_name}': {exc}"
                ) from exc
            if "/" in part:
                start, end = single, max_val
            else:
                start, end = single, single

        for v in range(start, end + 1, step):
            values.add(0 if (is_dow and v == 7) else v)

    if not values:
        raise ScheduleValidationError(f"Cron field '{field_name}' matched no values.")

    return frozenset(values), is_wildcard


def parse_cron_expression(expression: str) -> ParsedCronExpression:
    """Parse and validate a standard 5-field cron expression."""
    clean_expr = expression.strip() if isinstance(expression, str) else ""
    if not clean_expr:
        raise ScheduleValidationError("Cron expression must be a non-empty string.")

    fields = clean_expr.split()
    if len(fields) != 5:
        raise ScheduleValidationError(
            f"Cron expression must contain exactly 5 fields "
            f"(minute hour day_of_month month day_of_week), got {len(fields)}: '{clean_expr}'."
        )

    minutes, _ = _parse_cron_field(fields[0], 0, 59, field_name="minute")
    hours, _ = _parse_cron_field(fields[1], 0, 23, field_name="hour")
    doms, dom_wild = _parse_cron_field(fields[2], 1, 31, field_name="day_of_month")
    months, _ = _parse_cron_field(fields[3], 1, 12, field_name="month", names=_MONTH_NAMES)
    dows, dow_wild = _parse_cron_field(
        fields[4], 0, 6, field_name="day_of_week", names=_DOW_NAMES, is_dow=True
    )

    # Ensure day-of-month and month combination is calendar-satisfiable when dom is restricted
    if dom_wild or not dow_wild:
        pass
    else:
        max_possible_dom = max(_MAX_DAYS_PER_MONTH[m] for m in months)
        if not any(d <= max_possible_dom for d in doms):
            raise ScheduleValidationError(
                f"Cron expression '{clean_expr}' specifies impossible day-of-month {sorted(doms)} "
                f"for months {sorted(months)}."
            )

    return ParsedCronExpression(
        raw=clean_expr,
        minutes=tuple(sorted(minutes)),
        hours=tuple(sorted(hours)),
        days_of_month=doms,
        months=months,
        days_of_week=dows,
        dom_is_wildcard=dom_wild,
        dow_is_wildcard=dow_wild,
    )


def evaluate_most_recent_occurrence(
    expression: str,
    timezone_name: str,
    reference_time: datetime,
) -> datetime:
    """Calculate the most recent scheduled occurrence ``<= reference_time``.

    Parameters
    ----------
    expression:
        Validated 5-field cron expression.
    timezone_name:
        IANA timezone identifier (e.g. ``"America/Toronto"`` or ``"UTC"``).
    reference_time:
        Timezone-aware reference timestamp (typically current tick time).

    Returns
    -------
    datetime
        Timezone-aware :class:`datetime` in the configured ``timezone_name``
        representing the latest scheduled instant ``<= reference_time``.
    """
    if (
        not isinstance(reference_time, datetime)
        or reference_time.tzinfo is None
        or reference_time.tzinfo.utcoffset(reference_time) is None
    ):
        raise ScheduleValidationError("reference_time must be a timezone-aware datetime.")

    try:
        tz = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, KeyError, ValueError) as exc:
        raise ScheduleValidationError(f"Invalid IANA timezone '{timezone_name}': {exc}") from exc

    parsed = parse_cron_expression(expression)
    ref_utc = reference_time.astimezone(UTC)
    ref_local = ref_utc.astimezone(tz)

    start_date = ref_local.date() + timedelta(days=1)
    max_lookback_days = 366 * 5

    best_utc: datetime | None = None

    for day_offset in range(max_lookback_days):
        cand_date = start_date - timedelta(days=day_offset)
        _, days_in_month = calendar.monthrange(cand_date.year, cand_date.month)
        if cand_date.day > days_in_month:
            continue
        if not parsed.matches_date(cand_date):
            continue

        for hour in reversed(parsed.hours):
            for minute in reversed(parsed.minutes):
                local_dt = datetime(
                    cand_date.year,
                    cand_date.month,
                    cand_date.day,
                    hour,
                    minute,
                    0,
                    0,
                    tzinfo=tz,
                    fold=0,
                )
                cand_utc = local_dt.astimezone(UTC)
                if cand_utc <= ref_utc and (best_utc is None or cand_utc > best_utc):
                    best_utc = cand_utc

        if best_utc is not None:
            return best_utc.astimezone(tz)

    raise ScheduleValidationError(
        f"Could not find a scheduled occurrence for cron '{expression}' within 5 years."
    )


class TriggerDefinition(BaseModel):
    """Trigger configuration for a schedule definition (Phase 1 supports ``cron``)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    type: str
    expression: str
    timezone: str

    @model_validator(mode="after")
    def _validate_trigger(self) -> TriggerDefinition:
        clean_type = self.type.strip().lower() if isinstance(self.type, str) else ""
        if clean_type != "cron":
            raise ScheduleValidationError(
                f"Unsupported trigger type '{self.type}'. Only 'cron' is supported in Phase 1."
            )

        clean_expr = self.expression.strip() if isinstance(self.expression, str) else ""
        parse_cron_expression(clean_expr)

        clean_tz = self.timezone.strip() if isinstance(self.timezone, str) else ""
        if not clean_tz:
            raise ScheduleValidationError("Trigger timezone must be a non-empty IANA timezone.")
        try:
            ZoneInfo(clean_tz)
        except (ZoneInfoNotFoundError, KeyError, ValueError) as exc:
            raise ScheduleValidationError(
                f"Invalid IANA timezone identifier '{self.timezone}'."
            ) from exc

        object.__setattr__(self, "type", clean_type)
        object.__setattr__(self, "expression", clean_expr)
        object.__setattr__(self, "timezone", clean_tz)
        return self


class ScheduleDefinition(BaseModel):
    """Persistence-independent domain model for a schedule definition."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schedule_id: str
    name: str | None = None
    description: str | None = None
    enabled: bool = True
    trigger: TriggerDefinition
    jobs: list[str] = Field(default_factory=list)

    @field_validator("enabled", mode="before")
    @classmethod
    def _validate_enabled_bool(cls, value: Any) -> Any:
        if not isinstance(value, bool):
            raise ScheduleValidationError(
                f"Schedule 'enabled' must be a boolean, got {type(value).__name__}."
            )
        return value

    @model_validator(mode="after")
    def _validate_schedule(self) -> ScheduleDefinition:
        clean_id = self.schedule_id.strip() if isinstance(self.schedule_id, str) else ""
        try:
            require_not_empty(clean_id, "schedule_id")
        except ValueError as exc:
            raise ScheduleValidationError(str(exc)) from exc

        if not isinstance(self.jobs, list) or len(self.jobs) == 0:
            raise ScheduleValidationError(
                f"Schedule '{clean_id}' must specify at least one job ID in 'jobs'."
            )

        cleaned_jobs: list[str] = []
        for idx, job in enumerate(self.jobs):
            clean_job = job.strip() if isinstance(job, str) else ""
            if not clean_job:
                raise ScheduleValidationError(
                    f"Schedule '{clean_id}' contains an empty job ID at index {idx}."
                )
            cleaned_jobs.append(clean_job)

        clean_name = self.name.strip() if isinstance(self.name, str) and self.name.strip() else None
        clean_desc = (
            self.description.strip()
            if isinstance(self.description, str) and self.description.strip()
            else None
        )

        object.__setattr__(self, "schedule_id", clean_id)
        object.__setattr__(self, "name", clean_name)
        object.__setattr__(self, "description", clean_desc)
        object.__setattr__(self, "jobs", cleaned_jobs)
        return self


__all__ = [
    "ParsedCronExpression",
    "ScheduleDefinition",
    "TriggerDefinition",
    "evaluate_most_recent_occurrence",
    "parse_cron_expression",
]
