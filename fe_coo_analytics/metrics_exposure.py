
from __future__ import annotations
import pandas as pd
from .db import get_conn

def exposures_over_time(strategy: str | None = None) -> pd.DataFrame:
    con = get_conn(read_only=True)
    where, params = "", []
    if strategy:
        where = "WHERE strategy = ?"
        params = [strategy]
    df = con.execute(f"""
      SELECT date, strategy, gross_exposure, net_exposure
      FROM mart.daily_exposures
      {where}
      ORDER BY date, strategy;
    """, params).df()
    con.close()
    return df
