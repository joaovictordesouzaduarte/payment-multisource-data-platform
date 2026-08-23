"""Silver payment events → gold merchant and status marts."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Iterable, Mapping

CENTS = Decimal("0.01")
RATE = Decimal("0.000001")


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _amount(row: Mapping[str, Any]) -> Decimal:
    value = row["amount_usd"]
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _money(rows: Iterable[Mapping[str, Any]], key: str) -> Decimal:
    total = Decimal("0.00")
    for row in rows:
        if row.get(key):
            total += _amount(row)
    return total.quantize(CENTS, rounding=ROUND_HALF_UP)


def _rate(numerator: int, denominator: int) -> Decimal | None:
    if denominator == 0:
        return None
    return (Decimal(numerator) / Decimal(denominator)).quantize(
        RATE, rounding=ROUND_HALF_UP
    )


def merchant_daily_metrics(
    silver_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, date], list[Mapping[str, Any]]] = defaultdict(list)
    for row in silver_rows:
        groups[(str(row["merchant_id"]), _as_date(row["event_date"]))].append(row)

    mart: list[dict[str, Any]] = []
    for (merchant_id, event_date), items in groups.items():
        txn_count = len(items)
        captured_count = sum(1 for row in items if row.get("is_captured"))
        authorized_count = sum(1 for row in items if row.get("is_authorized"))
        failed_count = sum(1 for row in items if row.get("is_failed"))
        refunded_count = sum(1 for row in items if row.get("is_refunded"))
        gmv = _money(items, "is_revenue")
        aov = (
            (gmv / Decimal(captured_count)).quantize(CENTS, rounding=ROUND_HALF_UP)
            if captured_count
            else None
        )
        mart.append(
            {
                "merchant_id": merchant_id,
                "event_date": event_date,
                "txn_count": txn_count,
                "unique_users": len({str(row["user_id"]) for row in items}),
                "captured_count": captured_count,
                "authorized_count": authorized_count,
                "failed_count": failed_count,
                "refunded_count": refunded_count,
                "gmv_usd": gmv,
                "authorized_usd": _money(items, "is_authorized"),
                "failed_usd": _money(items, "is_failed"),
                "refunded_usd": _money(items, "is_refunded"),
                "aov_usd": aov,
                "capture_rate": _rate(captured_count, txn_count),
                "failure_rate": _rate(failed_count, txn_count),
                "large_ticket_count": sum(
                    1 for row in items if row.get("amount_band") == "large"
                ),
                "year": f"{event_date.year:04d}",
                "month": f"{event_date.month:02d}",
                "day": f"{event_date.day:02d}",
            }
        )
    return sorted(mart, key=lambda row: (row["event_date"], row["merchant_id"]))


def payment_status_daily(
    silver_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, date], list[Mapping[str, Any]]] = defaultdict(list)
    for row in silver_rows:
        groups[(str(row["status"]), _as_date(row["event_date"]))].append(row)

    mart: list[dict[str, Any]] = []
    for (status, event_date), items in groups.items():
        total = sum((_amount(row) for row in items), Decimal("0.00")).quantize(
            CENTS, rounding=ROUND_HALF_UP
        )
        mart.append(
            {
                "status": status,
                "event_date": event_date,
                "txn_count": len(items),
                "amount_usd": total,
                "unique_merchants": len({str(row["merchant_id"]) for row in items}),
                "unique_users": len({str(row["user_id"]) for row in items}),
                "year": f"{event_date.year:04d}",
                "month": f"{event_date.month:02d}",
                "day": f"{event_date.day:02d}",
            }
        )
    return sorted(mart, key=lambda row: (row["event_date"], row["status"]))


def to_gold(
    silver_rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = list(silver_rows)
    return merchant_daily_metrics(rows), payment_status_daily(rows)
