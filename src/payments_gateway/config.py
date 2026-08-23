"""Configuration for the payments gateway producer."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _load_env_file() -> None:
    """Load the nearest repo `.env` (no shell exports required)."""
    here = Path(__file__).resolve()
    for parent in (here.parent, *here.parents):
        candidate = parent / ".env"
        if candidate.is_file():
            load_dotenv(candidate, override=False)
            return
    load_dotenv(override=False)


_load_env_file()


@dataclass(frozen=True)
class Settings:
    """Settings loaded from `.env` / process environment."""

    aws_region: str
    kinesis_stream_name: str
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    batch_size: int = 100
    max_retries: int = 5
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> Settings:
        stream_name = os.getenv("KINESIS_STREAM_NAME", "").strip()
        access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
        missing = [
            name
            for name, value in (
                ("KINESIS_STREAM_NAME", stream_name),
                ("AWS_ACCESS_KEY_ID", access_key),
                ("AWS_SECRET_ACCESS_KEY", secret_key),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "Missing values in .env: "
                + ", ".join(missing)
                + ". Copy .env.example to .env and fill them in."
            )
        return cls(
            aws_region=os.getenv(
                "AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")
            ),
            kinesis_stream_name=stream_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            batch_size=int(os.getenv("KINESIS_BATCH_SIZE", "100")),
            max_retries=int(os.getenv("KINESIS_MAX_RETRIES", "5")),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )
