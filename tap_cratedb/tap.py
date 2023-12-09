"""CrateDB tap implementation, based on the PostgreSQL tap."""
from __future__ import annotations

from tap_postgres.client import (
    PostgresStream,
)
from tap_postgres.tap import TapPostgres


class TapCrateDB(TapPostgres):
    name = "tap-cratedb"
    default_stream_class = PostgresStream
