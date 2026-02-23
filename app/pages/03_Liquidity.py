import streamlit as st

from app_utils import render_sidebar
from fe_coo_analytics.db import get_conn

render_sidebar()
st.title("Liquidity Risk")

con = get_conn(read_only=True)
dates = [r[0] for r in con.execute("SELECT DISTINCT date FROM mart.daily_liquidity ORDER BY date;").fetchall()]
strategies = [r[0] for r in con.execute("SELECT DISTINCT strategy FROM mart.daily_liquidity ORDER BY 1;").fetchall()]
con.close()

c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    date = st.selectbox("Date", dates[::-1])
with c2:
    strategy = st.selectbox("Strategy", strategies)
with c3:
    only_flagged = st.checkbox("Only illiquid_flag = 1", value=False)

n = st.slider("Top N", 5, 50, 15)

con = get_conn(read_only=True)
df = con.execute("""
    SELECT date, strategy, ticker, shares, adv_shares, days_to_liquidate, illiquid_flag
    FROM mart.daily_liquidity
    WHERE date = ? AND strategy = ?
""", [date, strategy]).df()
con.close()

if only_flagged:
    df = df[df["illiquid_flag"] == 1]

df = df.sort_values("days_to_liquidate", ascending=False).head(n)

st.subheader("Most illiquid positions (days to liquidate)")
st.dataframe(df, use_container_width=True, height=350)

if len(df) > 0:
    st.bar_chart(df.set_index("ticker")["days_to_liquidate"])