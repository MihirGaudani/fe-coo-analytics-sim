from fe_coo_analytics.metrics_exposure import exposures_over_time

def test_exposures_columns_and_non_negative_gross():
    df = exposures_over_time()
    assert set(["date", "strategy", "gross_exposure", "net_exposure"]).issubset(df.columns)
    assert (df["gross_exposure"] >= 0).all()
