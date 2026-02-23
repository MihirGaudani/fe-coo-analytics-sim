from fe_coo_analytics.metrics_liquidity import most_illiquid

def test_most_illiquid_returns_rows():
    df = most_illiquid(n=5)
    assert len(df) == 5
    assert "days_to_liquidate" in df.columns
    assert df["days_to_liquidate"].isna().sum() == 0
