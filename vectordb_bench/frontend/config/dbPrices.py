from vectordb_bench import config
import logging
import pathlib
import ujson

log = logging.getLogger(__name__)
_db_prices_path = pathlib.Path(config.RESULTS_LOCAL_DIR, "dbPrices.json")

if _db_prices_path.exists():
    with open(_db_prices_path) as f:
        DB_DBLABEL_TO_PRICE = ujson.load(f)
else:
    log.debug(f"dbPrices.json not found at {_db_prices_path}; using empty price map. Create this file for Queries Per Dollar pricing.")
    DB_DBLABEL_TO_PRICE = {}
