import streamlit as st
import pandas as pd

from app_utils import render_sidebar
from fe_coo_analytics.db import get_conn

render_sidebar()
st.title("PnL Drilldown")

con = get_conn(read_only=True)
strategies = [r[0] for r in con.execute("SELECT DISTINCT strategy FROM mart.daily_pnl ORDER BY 1;").fetchall()]
tickers = [r[0] for r in con.execute("SELECT DISTINCT ticker FROM mart.daily_pnl ORDER BY 1;").fetchall()]
min_d, max_d = con.execute("SELECT MIN(date), MAX(date) FROM mart.daily_pnl;").fetchone()
con.close()

c1, c2, c3 = st.columns([1, 1, 1.4])
with c1:
    strategy = st.selectbox("Strategy", strategies)
with c2:
    ticker = st.selectbox("Ticker", tickers)
with c3:
    date_range = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)

start_d, end_d = date_range if isinstance(date_range, tuple) else (min_d, max_d)

@st.cache_data(show_spinner=False, ttl=30)
def load_drill(strategy, ticker, start_d, end_d):
    con = get_conn(read_only=True)
    df = con.execute("""
        SELECT date, shares_held, price_change, pnl
        FROM mart.daily_pnl
        WHERE strategy = ? AND ticker = ?
          AND date BETWEEN ? AND ?
        ORDER BY date;
    """, [strategy, ticker, start_d, end_d]).df()
    con.close()
    return df

df = load_drill(strategy, ticker, start_d, end_d)
df["cum_pnl"] = df["pnl"].cumsum()

st.subheader(f"{strategy} / {ticker}")

k1, k2 = st.columns(2)
k1.metric("Total PnL (selected range)", f"{df['pnl'].sum():,.0f}")
k2.metric("Max |daily PnL|", f"{df['pnl'].abs().max():,.0f}")

st.line_chart(df.set_index("date")[["pnl", "cum_pnl"]])

st.dataframe(df, use_container_width=True, height=350)
st.download_button("Download CSV", df.to_csv(index=False), file_name=f"pnl_{strategy}_{ticker}.csv")