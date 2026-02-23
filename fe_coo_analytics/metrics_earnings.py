
from __future__ import annotations
import pandas as pd
from .db import get_conn

def biggest_earnings_windows(n: int = 10) -> pd.DataFrame:
    con = get_conn(read_only=True)
    df = con.execute("""
      SELECT strategy, ticker, earnings_date, pnl_total_window
      FROM mart.earnings_window_pnl
      ORDER BY ABS(pnl_total_window) DESC
      LIMIT ?;
    """, [n]).df()
    con.close()
    return df
