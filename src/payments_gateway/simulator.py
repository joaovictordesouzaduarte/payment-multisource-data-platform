"""CLI simulator that emits realistic payment events to Kinesis."""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from payments_gateway.config import Settings
from payments_gateway.models import PaymentEvent, PaymentStatus
from payments_gateway.producer import PaymentEventProducer

logger = logging.getLogger(__name__)

MERCHANT_IDS = [f"mrc_{i:04d}" for i in range(1, 51)]
USER_IDS = [f"usr_{i:06d}" for i in range(1, 501)]
STATUS_WEIGHTS = [
    (PaymentStatus.CAPTURED, 0.70),
    (PaymentStatus.AUTHORIZED, 0.18),
    (PaymentStatus.FAILED, 0.10),
    (PaymentStatus.REFUNDED, 0.02),
]


def _weighted_status() -> PaymentStatus:
    roll = random.random()
    cumulative = 0.0
    for status, weight in STATUS_WEIGHTS:
        cumulative += weight
        if roll <= cumulative:
            return status
    return PaymentStatus.CAPTURED


def generate_event() -> PaymentEvent:
    amount = round(random.lognormvariate(mu=3.5, sigma=0.8), 2)
    amount = max(0.50, min(amount, 5000.0))
    return PaymentEvent(
        event_id=str(uuid4()),
        merchant_id=random.choice(MERCHANT_IDS),
        user_id=random.choice(USER_IDS),
        amount_usd=amount,
        currency="USD",
        status=_weighted_status(),
        event_timestamp=datetime.now(timezone.utc),
        schema_version=1,
        ingest_source="payment_gateway",
    )


def run_simulator(
    events_per_second: float,
    duration_seconds: float,
    total_events: int | None,
    producer: PaymentEventProducer | None = None,
) -> int:
    settings = Settings.from_env()
    producer = producer or PaymentEventProducer(settings=settings)

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    interval = 1.0 / events_per_second if events_per_second > 0 else 0.0
    started = time.monotonic()
    published = 0
    failed = 0
    batch: list[PaymentEvent] = []
    batch_flush_size = min(settings.batch_size, 100)

    logger.info(
        "Starting payments simulator",
        extra={
            "stream": settings.kinesis_stream_name,
            "region": settings.aws_region,
            "events_per_second": events_per_second,
            "duration_seconds": duration_seconds,
            "total_events": total_events,
        },
    )

    def flush() -> None:
        nonlocal published, failed, batch
        if not batch:
            return
        result = producer.publish(batch)
        published += result.succeeded
        failed += result.failed
        for event in batch:
            logger.info(event.model_dump_json())
        batch = []

    try:
        while True:
            elapsed = time.monotonic() - started
            if total_events is not None and published + len(batch) >= total_events:
                break
            if duration_seconds > 0 and elapsed >= duration_seconds:
                break

            batch.append(generate_event())
            if len(batch) >= batch_flush_size:
                flush()

            if interval > 0:
                time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Simulator interrupted by user")
    finally:
        flush()

    logger.info(
        "Simulator finished",
        extra={
            "published": published,
            "failed": failed,
            "elapsed_s": round(time.monotonic() - started, 2),
        },
    )
    return 0 if failed == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Simulate payment gateway events and publish them to Kinesis."
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Events per second (default: 10)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Run duration in seconds (default: 30). Ignored when --count is set.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Total events to publish (overrides --duration when set).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.rate <= 0:
        print("--rate must be > 0", file=sys.stderr)
        return 2
    return run_simulator(
        events_per_second=args.rate,
        duration_seconds=args.duration,
        total_events=args.count,
    )


if __name__ == "__main__":
    raise SystemExit(main())
