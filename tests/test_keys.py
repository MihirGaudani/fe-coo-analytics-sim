from fe_coo_analytics.validate import check_unique_key

def test_daily_positions_unique_key():
    res = check_unique_key("mart", "daily_positions", ["date", "strategy", "ticker"])
    assert res.passed, res.details

def test_daily_pnl_not_empty():
    # pnl table should have > 0 rows
    from fe_coo_analytics.validate import check_row_count
    res = check_row_count("mart", "daily_pnl", min_rows=1)
    assert res.passed, res.details
