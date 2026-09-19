"""Unit tests for silver → gold star schema."""

from datetime import date
from decimal import Decimal

from payments_lake.gold import DATE_END, DATE_START, dim_date, to_gold
from payments_lake.silver import to_silver


def _bronze(event_id, merchant_id, user_id, amount, status, ts):
    return {
        "event_id": event_id,
        "merchant_id": merchant_id,
        "user_id": user_id,
        "amount_usd": amount,
        "currency": "USD",
        "status": status,
        "event_timestamp": ts,
        "schema_version": 1,
        "ingest_source": "payment_gateway",
        "year": "2026",
        "month": "08",
        "day": "23",
        "hour": "02",
    }


SAMPLE = [
    _bronze(
        "5ef2c2fa-d3fc-4326-9a2d-ad1a0e2689a4",
        "mrc_0013",
        "usr_000132",
        "22.7",
        "captured",
        "2026-08-23T02:22:27.333120Z",
    ),
    _bronze(
        "c2020e17-881c-41fe-bba1-dceff04bb59a",
        "mrc_0013",
        "usr_000374",
        "40.55",
        "captured",
        "2026-08-23T02:22:27.401825Z",
    ),
    _bronze(
        "f580f5d7-60d6-49d5-b574-ec18a357d17a",
        "mrc_0018",
        "usr_000246",
        "11.21",
        "failed",
        "2026-08-23T02:22:27.424743Z",
    ),
    _bronze(
        "4483b205-3ba1-4abe-bc68-155343daf1e2",
        "mrc_0006",
        "usr_000156",
        "9.95",
        "authorized",
        "2026-08-23T02:22:27.355930Z",
    ),
    _bronze(
        "0bccd7d1-5ed0-4546-a1df-7a5a671bc4af",
        "mrc_0025",
        "usr_000123",
        "243.84",
        "captured",
        "2026-08-23T02:22:27.733259Z",
    ),
]


def _star():
    silver, rejected = to_silver(SAMPLE)
    assert rejected == []
    return to_gold(silver)


def test_fact_grain_and_revenue_flag():
    gold = _star()
    facts = gold["fact_payment"]
    assert len(facts) == 5
    assert len({row["event_id"] for row in facts}) == 5
    by_id = {row["event_id"]: row for row in facts}

    captured = by_id["5ef2c2fa-d3fc-4326-9a2d-ad1a0e2689a4"]
    assert captured["date_key"] == 20260823
    assert captured["merchant_id"] == "mrc_0013"
    assert captured["amount_usd"] == Decimal("22.70")
    assert captured["amount_cents"] == 2270
    assert captured["is_revenue"] is True
    assert captured["amount_band"] == "small"

    failed = by_id["f580f5d7-60d6-49d5-b574-ec18a357d17a"]
    assert failed["is_revenue"] is False
    assert failed["status"] == "failed"

    large = by_id["0bccd7d1-5ed0-4546-a1df-7a5a671bc4af"]
    assert large["amount_band"] == "large"
    assert large["is_revenue"] is True


def test_fact_foreign_keys_exist_in_dims():
    gold = _star()
    facts = gold["fact_payment"]
    date_keys = {row["date_key"] for row in gold["dim_date"]}
    merchants = {row["merchant_id"] for row in gold["dim_merchant"]}
    users = {row["user_id"] for row in gold["dim_user"]}
    statuses = {row["status"] for row in gold["dim_status"]}

    assert {row["date_key"] for row in facts} <= date_keys
    assert {row["merchant_id"] for row in facts} <= merchants
    assert {row["user_id"] for row in facts} <= users
    assert {row["status"] for row in facts} <= statuses
    assert merchants == {"mrc_0006", "mrc_0013", "mrc_0018", "mrc_0025"}
    assert len(gold["dim_user"]) == 5


def test_dim_status_revenue_only_for_captured():
    gold = _star()
    by_status = {row["status"]: row for row in gold["dim_status"]}
    assert set(by_status) == {"authorized", "captured", "failed", "refunded"}
    assert by_status["captured"]["is_revenue"] is True
    assert by_status["failed"]["is_revenue"] is False
    assert by_status["authorized"]["is_revenue"] is False
    assert by_status["refunded"]["is_revenue"] is False


def test_dim_date_calendar_range_and_weekend():
    rows = dim_date()
    assert rows[0]["full_date"] == DATE_START
    assert rows[-1]["full_date"] == DATE_END
    assert rows[0]["date_key"] == 20260101
    sunday = next(row for row in rows if row["full_date"] == date(2026, 8, 23))
    assert sunday["day_of_week"] == "Sunday"
    assert sunday["is_weekend"] is True
    assert sunday["month_name"] == "August"
