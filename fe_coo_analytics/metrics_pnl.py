from __future__ import annotations
import pandas as pd
from .db import get_conn

def pnl_by_day(strategy: str | None = None) -> pd.DataFrame:
    con = get_conn(read_only=True)
    where = ""
    params = []
    if strategy:
        where = "WHERE strategy = ?"
        params = [strategy]
    df = con.execute(f"""
      SELECT date, strategy, SUM(pnl) AS pnl
      FROM mart.daily_pnl
      {where}
      GROUP BY 1,2
      ORDER BY 1,2;
    """, params).df()
    con.close()
    return df

def top_pnl_movers(n: int = 10) -> pd.DataFrame:
    con = get_conn(read_only=True)
    df = con.execute("""
      SELECT date, strategy, ticker, pnl
      FROM mart.daily_pnl
      ORDER BY ABS(pnl) DESC
      LIMIT ?;
    """, [n]).df()
    con.close()
    return df
