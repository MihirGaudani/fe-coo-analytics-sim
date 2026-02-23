import streamlit as st
from fe_coo_analytics.db import get_conn

def render_sidebar():
    st.sidebar.header("FE-COO Sim")

    con = get_conn(read_only=True)
    last = con.execute("""
      SELECT run_ts, status, duration_seconds, regenerated_raw
      FROM ops.pipeline_runs
      ORDER BY run_ts DESC
      LIMIT 1;
    """).df()
    con.close()

    if len(last) == 1:
        r = last.iloc[0]
        st.sidebar.metric("Last run status", str(r["status"]))
        st.sidebar.write(f"**Run time (UTC):** {r['run_ts']}")
        st.sidebar.write(f"**Duration (s):** {r['duration_seconds']:.2f}")
        st.sidebar.write(f"**Regenerated raw:** {bool(r['regenerated_raw'])}")
    else:
        st.sidebar.warning("No pipeline runs logged yet.")