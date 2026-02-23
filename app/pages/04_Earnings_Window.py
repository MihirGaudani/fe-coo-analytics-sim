import streamlit as st

from app.app_utils import render_sidebar, load_distinct_strategies
from fe_coo_analytics.db import get_conn

render_sidebar()
st.title("Earnings Window Analysis")

strategies = load_distinct_strategies()
strategy = st.selectbox("Strategy", ["(all)"] + strategies)
n = st.slider("Top N", 5, 50, 15)

con = get_conn(read_only=True)
if strategy == "(all)":
    df = con.execute("""
        SELECT strategy, ticker, earnings_date, pnl_total_window
        FROM mart.earnings_window_pnl
        ORDER BY ABS(pnl_total_window) DESC
        LIMIT ?;
    """, [n]).df()
else:
    df = con.execute("""
        SELECT strategy, ticker, earnings_date, pnl_total_window
        FROM mart.earnings_window_pnl
        WHERE strategy = ?
        ORDER BY ABS(pnl_total_window) DESC
        LIMIT ?;
    """, [strategy, n]).df()
con.close()

st.dataframe(df, use_container_width=True, height=350)
st.caption("Useful for: “How did we perform through the earnings window?”")