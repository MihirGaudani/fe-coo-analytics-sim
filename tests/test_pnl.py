import pandas as pd
from fe_coo_analytics.metrics_pnl import pnl_by_day

def test_pnl_by_day_has_expected_columns():
    df = pnl_by_day()
    assert set(["date", "strategy", "pnl"]).issubset(df.columns)
    assert len(df) > 0

def test_pnl_by_day_no_null_pnl():
    df = pnl_by_day()
    assert df["pnl"].isna().sum() == 0
