from pathlib import Path
import os
import duckdb

DEFAULT_DB_PATH = Path(os.getenv("FE_COO_DB_PATH", "data/fe_coo.duckdb"))

def get_conn(read_only: bool = False, db_path: Path | str = DEFAULT_DB_PATH):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # If DB doesn't exist and caller asked for read-only, fall back to create it.
    if read_only and not db_path.exists():
        return duckdb.connect(str(db_path), read_only=False)

    return duckdb.connect(str(db_path), read_only=read_only)