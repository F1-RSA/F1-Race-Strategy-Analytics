"""Transformation step: builds the views of the core schema from the raw tables."""

from __future__ import annotations

import logging
from importlib import resources

import psycopg

logger = logging.getLogger(__name__)

LAPS_ENRICHED_SQL = "laps_enriched.sql"


def apply_view(conn: psycopg.Connection) -> None:
    """(Re)create core.laps_enriched. Safe to repeat: a view holds no data and is never stale."""
    sql = (resources.files("f1_ingest") / "sql" / LAPS_ENRICHED_SQL).read_text(encoding="utf-8")
    conn.execute(sql)
    logger.info("core.laps_enriched created or replaced")
