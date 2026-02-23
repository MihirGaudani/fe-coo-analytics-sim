from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
from .db import get_conn

@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str = ""

def check_table_exists(schema: str, table: str) -> CheckResult:
    con = get_conn(read_only=True)
    df = con.execute("""
      SELECT COUNT(*) AS n
      FROM duckdb_tables()
      WHERE schema_name = ? AND table_name = ?;
    """, [schema, table]).df()
    con.close()
    ok = int(df["n"][0]) == 1
    return CheckResult(f"exists:{schema}.{table}", ok, f"count={int(df['n'][0])}")

def check_row_count(schema: str, table: str, min_rows: int = 1) -> CheckResult:
    con = get_conn(read_only=True)
    n = con.execute(f"SELECT COUNT(*) AS n FROM {schema}.{table};").fetchone()[0]
    con.close()
    ok = n >= min_rows
    return CheckResult(f"min_rows:{schema}.{table}", ok, f"rows={n}, min_rows={min_rows}")

def check_unique_key(schema: str, table: str, key_cols: list[str]) -> CheckResult:
    con = get_conn(read_only=True)
    cols = ", ".join(key_cols)
    q = f"""
      SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT ({' || '.join([f'CAST({c} AS VARCHAR)' for c in key_cols])})) AS distinct_keys
      FROM {schema}.{table};
    """
    # Better: count duplicates directly (more robust)
    q2 = f"""
      SELECT COUNT(*) AS dup_rows
      FROM (
        SELECT {cols}, COUNT(*) c
        FROM {schema}.{table}
        GROUP BY {cols}
        HAVING COUNT(*) > 1
      );
    """
    dup = con.execute(q2).fetchone()[0]
    con.close()
    ok = dup == 0
    return CheckResult(f"unique_key:{schema}.{table}({cols})", ok, f"dup_groups={dup}")
