import streamlit as st
import pandas as pd

from app.app_utils import render_sidebar, load_distinct_strategies, load_date_bounds
from fe_coo_analytics.db import get_conn

render_sidebar()
st.title("Overview")

strategies = load_distinct_strategies()
min_d, max_d = load_date_bounds("mart.daily_exposures")

c1, c2 = st.columns([2, 1])
with c1:
    chosen_strats = st.multiselect("Strategies", strategies, default=strategies)
with c2:
    date_range = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)

start_d, end_d = date_range if isinstance(date_range, tuple) else (min_d, max_d)

@st.cache_data(show_spinner=False, ttl=30)
def load_overview_data(chosen_strats, start_d, end_d):
    con = get_conn(read_only=True)

    exp = con.execute("""
        SELECT date, strategy, gross_exposure, net_exposure
        FROM mart.daily_exposures
        WHERE strategy IN (SELECT UNNEST(?))
          AND date BETWEEN ? AND ?
        ORDER BY date, strategy;
    """, [chosen_strats, start_d, end_d]).df()

    pnl = con.execute("""
        SELECT date, strategy, SUM(pnl) AS pnl
        FROM mart.daily_pnl
        WHERE strategy IN (SELECT UNNEST(?))
          AND date BETWEEN ? AND ?
        GROUP BY 1,2
        ORDER BY 1,2;
    """, [chosen_strats, start_d, end_d]).df()

    latest = con.execute("""
        SELECT MAX(date) FROM mart.daily_exposures
    """).fetchone()[0]

    kpis = con.execute("""
        WITH latest_exp AS (
          SELECT * FROM mart.daily_exposures WHERE date = ? AND strategy IN (SELECT UNNEST(?))
        ),
        latest_pnl AS (
          SELECT SUM(pnl) AS total_pnl FROM mart.daily_pnl WHERE date = ? AND strategy IN (SELECT UNNEST(?))
        ),
        liq AS (
          SELECT SUM(illiquid_flag) AS illiquid_positions
          FROM mart.daily_liquidity
          WHERE date = ? AND strategy IN (SELECT UNNEST(?))
        )
        SELECT
          (SELECT total_pnl FROM latest_pnl) AS total_pnl,
          (SELECT SUM(gross_exposure) FROM latest_exp) AS gross_exposure,
          (SELECT SUM(net_exposure) FROM latest_exp) AS net_exposure,
          (SELECT illiquid_positions FROM liq) AS illiquid_positions
    """, [latest, chosen_strats, latest, chosen_strats, latest, chosen_strats]).df()

    con.close()
    return exp, pnl, kpis, latest

exp_df, pnl_df, kpis_df, latest_date = load_overview_data(chosen_strats, start_d, end_d)

# KPI cards
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total PnL (latest day)", f"{float(kpis_df['total_pnl'][0]):,.0f}")
k2.metric("Gross exposure", f"{float(kpis_df['gross_exposure'][0]):,.0f}")
k3.metric("Net exposure", f"{float(kpis_df['net_exposure'][0]):,.0f}")
k4.metric("Illiquid positions", int(kpis_df["illiquid_positions"][0]))

st.caption(f"Latest exposure day in DB: {latest_date}")

st.markdown("## Exposure")
exp_pivot_g = exp_df.pivot(index="date", columns="strategy", values="gross_exposure").fillna(0)
exp_pivot_n = exp_df.pivot(index="date", columns="strategy", values="net_exposure").fillna(0)

t1, t2 = st.tabs(["Gross Exposure", "Net Exposure"])
with t1:
    st.line_chart(exp_pivot_g)
with t2:
    st.line_chart(exp_pivot_n)

st.markdown("## PnL")
pnl_pivot = pnl_df.pivot(index="date", columns="strategy", values="pnl").fillna(0)
st.line_chart(pnl_pivot)

with st.expander("Show underlying tables"):
    st.dataframe(exp_df, use_container_width=True, height=250)
    st.dataframe(pnl_df, use_container_width=True, height=250)