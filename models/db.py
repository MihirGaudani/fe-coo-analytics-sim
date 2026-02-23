import duckdb
from pathlib import Path

DB_PATH = Path("data/fe_coo.duckdb")

def get_conn(read_only: bool = False):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH), read_only=read_only)
