import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv()
import streamlit as st
import pandas as pd
from fe_coo_analytics.db import get_conn


@st.cache_data(show_spinner=False, ttl=30)
def load_last_pipeline_run() -> pd.DataFrame:
    con = get_conn(read_only=True)
    df = con.execute("""
        SELECT
          run_ts,
          status,
          regenerated_raw,
          duration_seconds,
          models_ran,
          error_message
        FROM ops.pipeline_runs
        ORDER BY run_ts DESC
        LIMIT 1;
    """).df()
    con.close()
    return df


@st.cache_data(show_spinner=False, ttl=60)
def load_distinct_strategies() -> list[str]:
    con = get_conn(read_only=True)
    rows = con.execute("SELECT DISTINCT strategy FROM mart.daily_pnl ORDER BY 1;").fetchall()
    con.close()
    return [r[0] for r in rows]


@st.cache_data(show_spinner=False, ttl=60)
def load_date_bounds(table: str = "mart.daily_exposures") -> tuple:
    con = get_conn(read_only=True)
    mn, mx = con.execute(f"SELECT MIN(date), MAX(date) FROM {table};").fetchone()
    con.close()
    return mn, mx


def render_sidebar() -> None:
    st.sidebar.title("FE-COO Sim")

    last = load_last_pipeline_run()
    if len(last) == 1:
        r = last.iloc[0]
        status = str(r["status"])
        st.sidebar.markdown("### Pipeline")
        st.sidebar.write(f"**Status:** {status}")
        st.sidebar.write(f"**Last run (UTC):** {r['run_ts']}")
        st.sidebar.write(f"**Duration (s):** {float(r['duration_seconds']):.2f}")
        st.sidebar.write(f"**Regenerated raw:** {bool(r['regenerated_raw'])}")

        if r["error_message"] is not None:
            with st.sidebar.expander("Error"):
                st.sidebar.code(str(r["error_message"]))
    else:
        st.sidebar.warning("No pipeline runs logged yet.")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Refresh marts")
    st.sidebar.code("python -m pipelines.build_mart_flow", language="bash")
    st.sidebar.code(
        "python -c \"from pipelines.build_mart_flow import build_mart; build_mart(regenerate_raw=True)\"",
        language="bash",
    )