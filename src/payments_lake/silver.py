"""Bronze NDJSON → typed silver payment events and quarantine."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Iterable, Mapping
from uuid import UUID

VALID_STATUSES = frozenset({"authorized", "captured", "failed", "refunded"})
REQUIRED_FIELDS = (
    "event_id",
    "merchant_id",
    "user_id",
    "amount_usd",
    "status",
    "event_timestamp",
)
INGEST_PATH_RE = re.compile(
    r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})/hour=(\d{1,2})"
)
CENTS = Decimal("0.01")
_INGEST_KEYS = ("year", "month", "day", "hour")
_INGEST_ALIASES = (
    "ingest_year",
    "ingest_month",
    "ingest_day",
    "ingest_hour",
)


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _two(value: Any) -> str:
    return str(int(value)).zfill(2)


def _ingest_from_path(path: str) -> dict[str, str]:
    match = INGEST_PATH_RE.search(path or "")
    if not match:
        return {}
    year, month, day, hour = match.groups()
    return {
        "ingest_year": year,
        "ingest_month": month.zfill(2),
        "ingest_day": day.zfill(2),
        "ingest_hour": hour.zfill(2),
    }


def _ingest_partitions(row: Mapping[str, Any]) -> dict[str, str]:
    if all(not _blank(row.get(key)) for key in _INGEST_KEYS):
        return {
            "ingest_year": str(int(row["year"])).zfill(4),
            "ingest_month": _two(row["month"]),
            "ingest_day": _two(row["day"]),
            "ingest_hour": _two(row["hour"]),
        }
    if all(not _blank(row.get(key)) for key in _INGEST_ALIASES):
        return {
            "ingest_year": str(int(row["ingest_year"])).zfill(4),
            "ingest_month": _two(row["ingest_month"]),
            "ingest_day": _two(row["ingest_day"]),
            "ingest_hour": _two(row["ingest_hour"]),
        }
    path = str(row.get("_source") or row.get("input_file_name") or "")
    return _ingest_from_path(path)


def _parse_amount(value: Any) -> Decimal | None:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if amount <= 0:
        return None
    return amount.quantize(CENTS, rounding=ROUND_HALF_UP)


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _valid_uuid(value: Any) -> bool:
    try:
        UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        return False
    return True


def _amount_band(amount: Decimal) -> str:
    if amount < Decimal("10"):
        return "micro"
    if amount < Decimal("50"):
        return "small"
    if amount < Decimal("100"):
        return "medium"
    return "large"


def _rejection_reason(row: Mapping[str, Any]) -> str | None:
    missing = [field for field in REQUIRED_FIELDS if _blank(row.get(field))]
    if missing:
        return "missing_fields:" + ",".join(missing)
    if not _valid_uuid(row.get("event_id")):
        return "invalid_event_id"
    if _parse_amount(row.get("amount_usd")) is None:
        try:
            raw = Decimal(str(row.get("amount_usd")))
        except (InvalidOperation, TypeError, ValueError):
            return "invalid_amount"
        if raw <= 0:
            return "non_positive_amount"
        return "invalid_amount"
    status = str(row.get("status", "")).strip().lower()
    if status not in VALID_STATUSES:
        return "invalid_status"
    currency = str(row.get("currency") or "USD").strip()
    if len(currency) != 3:
        return "invalid_currency"
    if _parse_timestamp(row.get("event_timestamp")) is None:
        return "invalid_event_timestamp"
    return None


def _ingest_lag_hours(event_ts: datetime, ingest: Mapping[str, str]) -> int | None:
    if not ingest:
        return None
    ingest_dt = datetime(
        int(ingest["ingest_year"]),
        int(ingest["ingest_month"]),
        int(ingest["ingest_day"]),
        int(ingest["ingest_hour"]),
        tzinfo=timezone.utc,
    )
    event_hour = event_ts.replace(minute=0, second=0, microsecond=0)
    return int((ingest_dt - event_hour).total_seconds() // 3600)


def _to_silver_row(row: Mapping[str, Any]) -> dict[str, Any]:
    event_ts = _parse_timestamp(row["event_timestamp"])
    amount = _parse_amount(row["amount_usd"])
    assert event_ts is not None and amount is not None
    status = str(row["status"]).strip().lower()
    currency = str(row.get("currency") or "USD").strip().upper()
    ingest = _ingest_partitions(row)
    event_date = event_ts.date()
    try:
        schema_version = int(row.get("schema_version") or 1)
    except (TypeError, ValueError):
        schema_version = 1
    return {
        "event_id": str(row["event_id"]).strip(),
        "merchant_id": str(row["merchant_id"]).strip(),
        "user_id": str(row["user_id"]).strip(),
        "amount_usd": amount,
        "amount_cents": int((amount * 100).to_integral_value()),
        "currency": currency,
        "status": status,
        "event_timestamp": event_ts,
        "schema_version": schema_version,
        "ingest_source": str(row.get("ingest_source") or "payment_gateway"),
        "is_captured": status == "captured",
        "is_authorized": status == "authorized",
        "is_failed": status == "failed",
        "is_refunded": status == "refunded",
        "is_revenue": status == "captured",
        "amount_band": _amount_band(amount),
        "event_date": event_date,
        "event_hour": event_ts.hour,
        "ingest_year": ingest.get("ingest_year"),
        "ingest_month": ingest.get("ingest_month"),
        "ingest_day": ingest.get("ingest_day"),
        "ingest_hour": ingest.get("ingest_hour"),
        "ingest_lag_hours": _ingest_lag_hours(event_ts, ingest),
        "year": f"{event_date.year:04d}",
        "month": f"{event_date.month:02d}",
        "day": f"{event_date.day:02d}",
    }


def _dedup(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        current = latest.get(row["event_id"])
        if current is None or row["event_timestamp"] >= current["event_timestamp"]:
            latest[row["event_id"]] = row
    return list(latest.values())


def to_silver(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (accepted silver rows, rejected bronze rows with reason)."""
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in rows:
        material = dict(row)
        reason = _rejection_reason(material)
        if reason:
            rejected.append({**material, "rejection_reason": reason})
            continue
        accepted.append(_to_silver_row(material))
    return _dedup(accepted), rejected


def to_spark_record(row: Mapping[str, Any]) -> dict[str, Any]:
    """JSON-friendly record for Spark createDataFrame inference."""
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        elif isinstance(value, date):
            out[key] = value.isoformat()
        elif isinstance(value, Decimal):
            out[key] = float(value)
        else:
            out[key] = value
    return out
