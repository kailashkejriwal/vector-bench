"""Placeholder DB configs used when auto_start is enabled (replaced by provisioner)."""

from vectordb_bench.backend.clients import DB

# Minimal valid config per DB so validation passes; orchestrator replaces with real connection.
PLACEHOLDER_CONFIG: dict[DB, dict] = {
    DB.PgVector: {
        "host": "127.0.0.1",
        "port": 5432,
        "user_name": "postgres",
        "password": "postgres",
        "db_name": "vectordbbench",
        "table_name": "vdbbench_table_test",
    },
    DB.Milvus: {
        "uri": "http://127.0.0.1:19530",
    },
    DB.Clickhouse: {
        "host": "127.0.0.1",
        "port": 8123,
        "user": "default",
        "password": "vectordbbench",
        "db_name": "default",
        "secure": False,
    },
    DB.QdrantLocal: {
        "url": "http://127.0.0.1:6333",
    },
}


def get_placeholder_config(db: DB) -> dict | None:
    """Return placeholder config dict for the DB, or None if not defined."""
    return PLACEHOLDER_CONFIG.get(db)
