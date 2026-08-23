"""Unit tests for bronze → silver transforms."""

from datetime import date, datetime
from decimal import Decimal

from payments_lake.silver import to_silver, to_spark_record


def _row(**overrides):
    base = {
        "event_id": "5ef2c2fa-d3fc-4326-9a2d-ad1a0e2689a4",
        "merchant_id": "mrc_0013",
        "user_id": "usr_000132",
        "amount_usd": "22.7",
        "currency": "usd",
        "status": "captured",
        "event_timestamp": "2026-08-23T02:22:27.333120Z",
        "schema_version": "1",
        "ingest_source": "payment_gateway",
        "year": "2026",
        "month": "08",
        "day": "23",
        "hour": "03",
    }
    base.update(overrides)
    return base


def test_silver_types_flags_and_event_time_partitions():
    accepted, rejected = to_silver([_row()])
    assert rejected == []
    assert len(accepted) == 1
    row = accepted[0]
    assert row["amount_usd"] == Decimal("22.70")
    assert row["amount_cents"] == 2270
    assert row["currency"] == "USD"
    assert row["is_captured"] is True
    assert row["is_revenue"] is True
    assert row["amount_band"] == "small"
    assert row["event_date"] == date(2026, 8, 23)
    assert row["event_hour"] == 2
    assert row["year"] == "2026"
    assert row["month"] == "08"
    assert row["day"] == "23"
    assert row["ingest_hour"] == "03"
    assert row["ingest_lag_hours"] == 1


def test_amount_bands():
    cases = [
        ("3.22", "micro"),
        ("9.95", "micro"),
        ("50.00", "medium"),
        ("243.84", "large"),
    ]
    for amount, band in cases:
        accepted, rejected = to_silver([_row(amount_usd=amount)])
        assert rejected == []
        assert accepted[0]["amount_band"] == band


def test_failed_is_not_revenue():
    accepted, _ = to_silver([_row(status="failed", amount_usd="11.21")])
    assert accepted[0]["is_failed"] is True
    assert accepted[0]["is_revenue"] is False


def test_ingest_partitions_from_firehose_path():
    accepted, _ = to_silver(
        [
            _row(
                year=None,
                month=None,
                day=None,
                hour=None,
                _source=(
                    "s3://bucket/payments/raw/year=2026/month=08/"
                    "day=22/hour=23/obj.gz"
                ),
            )
        ]
    )
    assert accepted[0]["ingest_year"] == "2026"
    assert accepted[0]["ingest_month"] == "08"
    assert accepted[0]["ingest_day"] == "22"
    assert accepted[0]["ingest_hour"] == "23"


def test_dedup_keeps_latest_event_timestamp():
    older = _row(event_timestamp="2026-08-23T02:22:27.333120Z", amount_usd="10.00")
    newer = _row(event_timestamp="2026-08-23T02:22:28.333120Z", amount_usd="99.00")
    accepted, rejected = to_silver([older, newer])
    assert rejected == []
    assert len(accepted) == 1
    assert accepted[0]["amount_usd"] == Decimal("99.00")


def test_quarantine_invalid_rows():
    rows = [
        _row(event_id="not-a-uuid"),
        _row(event_id="4483b205-3ba1-4abe-bc68-155343daf1e2", amount_usd="-1"),
        _row(event_id="0262780c-240e-4718-9691-9980f0bc59a3", status="charged"),
        _row(event_id="71ea6ac5-c206-4711-a3ee-ba81c42f5aba", merchant_id=""),
    ]
    accepted, rejected = to_silver(rows)
    assert accepted == []
    reasons = {row["rejection_reason"] for row in rejected}
    assert "invalid_event_id" in reasons
    assert "non_positive_amount" in reasons
    assert "invalid_status" in reasons
    assert "missing_fields:merchant_id" in reasons


def test_to_spark_record_serializes_decimal_and_dates():
    accepted, _ = to_silver([_row()])
    record = to_spark_record(accepted[0])
    assert isinstance(record["amount_usd"], float)
    assert record["event_date"] == "2026-08-23"
    assert record["event_timestamp"].startswith("2026-08-23T02:22:27")
    assert isinstance(datetime.fromisoformat(record["event_timestamp"]), datetime)
