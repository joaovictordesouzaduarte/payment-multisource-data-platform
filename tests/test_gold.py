"""Unit tests for silver → gold marts."""

from decimal import Decimal

from payments_lake.gold import to_gold
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


def test_merchant_daily_from_sample_events():
    silver, rejected = to_silver(SAMPLE)
    assert rejected == []
    merchant, _status = to_gold(silver)
    by_merchant = {row["merchant_id"]: row for row in merchant}

    m13 = by_merchant["mrc_0013"]
    assert m13["txn_count"] == 2
    assert m13["unique_users"] == 2
    assert m13["captured_count"] == 2
    assert m13["gmv_usd"] == Decimal("63.25")
    assert m13["aov_usd"] == Decimal("31.63")
    assert m13["capture_rate"] == Decimal("1.000000")
    assert m13["failure_rate"] == Decimal("0.000000")

    m18 = by_merchant["mrc_0018"]
    assert m18["gmv_usd"] == Decimal("0.00")
    assert m18["failed_count"] == 1
    assert m18["failed_usd"] == Decimal("11.21")
    assert m18["aov_usd"] is None
    assert m18["failure_rate"] == Decimal("1.000000")

    m25 = by_merchant["mrc_0025"]
    assert m25["large_ticket_count"] == 1
    assert m25["gmv_usd"] == Decimal("243.84")


def test_status_daily_mix():
    silver, _ = to_silver(SAMPLE)
    _merchant, status = to_gold(silver)
    by_status = {row["status"]: row for row in status}
    assert by_status["captured"]["txn_count"] == 3
    assert by_status["captured"]["amount_usd"] == Decimal("307.09")
    assert by_status["captured"]["unique_merchants"] == 2
    assert by_status["failed"]["txn_count"] == 1
    assert by_status["authorized"]["txn_count"] == 1
