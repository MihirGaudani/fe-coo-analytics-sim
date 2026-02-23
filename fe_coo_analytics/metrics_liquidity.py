
from __future__ import annotations
import pandas as pd
from .db import get_conn

def most_illiquid(date: str | None = None, n: int = 10) -> pd.DataFrame:
    con = get_conn(read_only=True)
    if date:
        df = con.execute("""
          SELECT date, strategy, ticker, shares, adv_shares, days_to_liquidate, illiquid_flag
          FROM mart.daily_liquidity
          WHERE date = ?
          ORDER BY days_to_liquidate DESC
          LIMIT ?;
        """, [date, n]).df()
    else:
        df = con.execute("""
          SELECT date, strategy, ticker, shares, adv_shares, days_to_liquidate, illiquid_flag
          FROM mart.daily_liquidity
          ORDER BY days_to_liquidate DESC
          LIMIT ?;
        """, [n]).df()
    con.close()
    return df
