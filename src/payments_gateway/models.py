"""Payment event data contract for bronze landing."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class PaymentStatus(str, Enum):
    AUTHORIZED = "authorized"
    CAPTURED = "captured"
    FAILED = "failed"
    REFUNDED = "refunded"


class PaymentEvent(BaseModel):
    """Canonical payment gateway event published to Kinesis."""

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    merchant_id: str
    user_id: str
    amount_usd: float = Field(gt=0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    status: PaymentStatus
    event_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    schema_version: int = Field(default=1, ge=1)
    ingest_source: str = Field(default="payment_gateway")

    @field_validator("currency")
    @classmethod
    def currency_upper(cls, value: str) -> str:
        return value.upper()

    @field_validator("event_timestamp")
    @classmethod
    def ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def partition_key(self) -> str:
        """Kinesis partition key — merchant_id keeps per-merchant ordering."""
        return self.merchant_id

    def to_kinesis_data(self) -> bytes:
        """Serialize as newline-delimited JSON bytes for Firehose aggregation."""
        return (self.model_dump_json() + "\n").encode("utf-8")

    @classmethod
    def json_schema_export(cls) -> dict[str, Any]:
        return cls.model_json_schema()
