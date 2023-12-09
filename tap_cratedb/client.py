"""CrateDB tap SQL client handling, based on the PostgreSQL tap."""
from __future__ import annotations

from tap_postgres.client import PostgresConnector


class CrateDBConnector(PostgresConnector):
    pass
