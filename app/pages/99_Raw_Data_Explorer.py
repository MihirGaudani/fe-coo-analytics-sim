import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

import streamlit as st
import pandas as pd
from fe_coo_analytics.db import get_conn
from app.app_utils import render_sidebar
render_sidebar()

st.set_page_config(page_title="Raw Data Explorer", layout="wide")
st.title("Raw Data Explorer (Simulated FE-COO Inputs)")

@st.cache_data(show_spinner=False)
def load_table(sql: str) -> pd.DataFrame:
    con = get_conn(read_only=True)
    df = con.execute(sql).df()
    con.close()
    return df

table = st.selectbox(
    "Choose a raw table",
    ["raw.trades", "raw.prices", "raw.security_master", "raw.liquidity", "raw.earnings_calendar"],
)

limit = st.slider("Row limit", 50, 5000, 500, step=50)

if table == "raw.trades":
    df = load_table(f"SELECT * FROM raw.trades ORDER BY timestamp DESC LIMIT {limit}")
    st.subheader("Trades")
    st.write("Filters:")
    c1, c2, c3 = st.columns(3)
    strategies = sorted(df["strategy"].unique())
    tickers = sorted(df["ticker"].unique())
    strat = c1.multiselect("Strategy", strategies, default=strategies)
    tick = c2.multiselect("Ticker", tickers, default=tickers[:10])
    side = c3.multiselect("Side", sorted(df["side"].unique()), default=sorted(df["side"].unique()))

    f = df[df["strategy"].isin(strat) & df["ticker"].isin(tick) & df["side"].isin(side)]
    st.dataframe(f, use_container_width=True, height=400)

    st.subheader("Trade volume by day")
    vol = (
        f.assign(trade_date=pd.to_datetime(f["trade_date"]))
         .groupby("trade_date")["quantity"]
         .sum()
         .reset_index()
         .sort_values("trade_date")
    )
    st.line_chart(vol, x="trade_date", y="quantity")

elif table == "raw.prices":
    df = load_table(f"SELECT * FROM raw.prices ORDER BY date DESC, ticker LIMIT {limit}")
    st.subheader("Prices")
    tickers = sorted(df["ticker"].unique())
    chosen = st.multiselect("Tickers to plot", tickers, default=tickers[:5])
    f = df[df["ticker"].isin(chosen)].copy()
    f["date"] = pd.to_datetime(f["date"])
    st.dataframe(f.sort_values(["date","ticker"]), use_container_width=True, height=400)

    st.subheader("Price chart")
    pivot = f.pivot_table(index="date", columns="ticker", values="close").sort_index()
    st.line_chart(pivot)

elif table == "raw.security_master":
    df = load_table(f"SELECT * FROM raw.security_master LIMIT {limit}")
    st.subheader("Security Master (Ticker metadata)")
    st.dataframe(df, use_container_width=True, height=450)
    st.subheader("Tickers by sector")
    counts = df.groupby("sector")["ticker"].count().reset_index(name="n_tickers").sort_values("n_tickers", ascending=False)
    st.bar_chart(counts, x="sector", y="n_tickers")

elif table == "raw.liquidity":
    df = load_table(f"SELECT * FROM raw.liquidity LIMIT {limit}")
    st.subheader("Liquidity (ADV shares)")
    st.dataframe(df, use_container_width=True, height=450)
    st.subheader("ADV distribution")
    st.write("Higher ADV = more liquid (easier to trade).")
    st.bar_chart(df.sort_values("adv_shares", ascending=False).head(20), x="ticker", y="adv_shares")

else:  # earnings_calendar
    df = load_table(f"SELECT * FROM raw.earnings_calendar LIMIT {limit}")
    st.subheader("Earnings Calendar")
    df["earnings_date"] = pd.to_datetime(df["earnings_date"])
    st.dataframe(df.sort_values("earnings_date"), use_container_width=True, height=450)
    st.subheader("Earnings dates count by week")
    df["week"] = df["earnings_date"].dt.to_period("W").astype(str)
    counts = df.groupby("week")["ticker"].count().reset_index(name="n")
    st.bar_chart(counts, x="week", y="n")
