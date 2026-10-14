"""
Loads landed micro-batch files into Snowflake.

    python -m streaming.snowflake_loader

For every pending file in data/landing/:
    PUT  file  ->  internal stage @RAW.CARD_TXN_STAGE
    COPY INTO RAW.CARD_TRANSACTIONS (or RAW.CARD_TRANSACTIONS_DLQ for dead letters)
then the file is moved to data/landing/loaded/.

Idempotent: Snowflake remembers which staged files it has already loaded
(load metadata, 64 days), so re-running after a failure never loads a file twice.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from streaming.config import LANDING_DIR, SNOWFLAKE

log = logging.getLogger("snowflake_loader")

TARGETS = {
    "batch_": "RAW.CARD_TRANSACTIONS",
    "dlq_": "RAW.CARD_TRANSACTIONS_DLQ",
}
STAGE = "@RAW.CARD_TXN_STAGE"


def pending_files(landing_dir: Path = LANDING_DIR) -> list[tuple[Path, str]]:
    """Files waiting to be loaded, oldest first, with their target table."""
    files = []
    for path in sorted(Path(landing_dir).glob("*.json.gz")):
        for prefix, table in TARGETS.items():
            if path.name.startswith(prefix):
                files.append((path, table))
    return files


def copy_sql(table: str, file_name: str) -> str:
    return (
        f"COPY INTO {table} (payload, source_file, file_row_number) "
        f"FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER FROM {STAGE}) "
        f"FILES = ('{file_name}') "
        f"FILE_FORMAT = (FORMAT_NAME = 'RAW.JSON_GZ') "
        f"ON_ERROR = 'ABORT_STATEMENT'"
    )


def connect():
    """Connect with key-pair authentication (Snowflake service users can't use passwords)."""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(Path(SNOWFLAKE["private_key_file"]).read_bytes(),
                                             password=None)
    private_key = key.private_bytes(encoding=serialization.Encoding.DER,
                                    format=serialization.PrivateFormat.PKCS8,
                                    encryption_algorithm=serialization.NoEncryption())
    params = {k: v for k, v in SNOWFLAKE.items() if k != "private_key_file"}
    return snowflake.connector.connect(private_key=private_key, **params)


def load_pending(landing_dir: Path = LANDING_DIR) -> dict:
    files = pending_files(landing_dir)
    if not files:
        log.info("nothing to load")
        return {"files_loaded": 0, "rows_loaded": 0}

    loaded_dir = Path(landing_dir) / "loaded"
    loaded_dir.mkdir(exist_ok=True)
    rows_total = 0
    with connect() as conn, conn.cursor() as cur:
        for path, table in files:
            cur.execute(f"PUT 'file://{path.resolve().as_posix()}' {STAGE} "
                        f"AUTO_COMPRESS = FALSE OVERWRITE = FALSE")
            cur.execute(copy_sql(table, path.name))
            # COPY returns one row per file: (file, status, rows_parsed, rows_loaded, ...)
            results = cur.fetchall()
            rows = sum(r[3] for r in results if len(r) > 3 and isinstance(r[3], int))
            status = results[0][1] if results and len(results[0]) > 1 else "already loaded"
            log.info("%s -> %s: %s (%s rows)", path.name, table, status, rows)
            rows_total += rows
            shutil.move(str(path), loaded_dir / path.name)

    return {"files_loaded": len(files), "rows_loaded": rows_total}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    print(load_pending())
