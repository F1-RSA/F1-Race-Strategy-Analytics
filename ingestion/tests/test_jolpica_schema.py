import psycopg
import pytest

PRIMARY_KEY_SQL = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indisprimary AND i.indrelid = %s::regclass
    ORDER BY array_position(i.indkey::int[], a.attnum::int)
"""


@pytest.mark.integration
@pytest.mark.parametrize(
    ("table", "key"),
    [
        ("raw.jolpica_races", ["season", "round"]),
        ("raw.jolpica_results", ["season", "round", "driver_id"]),
    ],
)
def test_jolpica_tables_exist_with_the_documented_primary_key(
    conn: psycopg.Connection, table: str, key: list[str]
) -> None:
    columns = [row[0] for row in conn.execute(PRIMARY_KEY_SQL, (table,)).fetchall()]
    assert columns == key


@pytest.mark.integration
def test_jolpica_tables_have_the_raw_layer_columns(conn: psycopg.Connection) -> None:
    for table in ("jolpica_races", "jolpica_results"):
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'raw' AND table_name = %s",
            (table,),
        ).fetchall()
        assert {"payload", "_ingested_at", "_run_id"} <= {row[0] for row in rows}
