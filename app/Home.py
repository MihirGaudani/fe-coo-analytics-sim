import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv()
import streamlit as st
from app.app_utils import render_sidebar
render_sidebar()
import pandas as pd

from fe_coo_analytics.db import get_conn

st.set_page_config(page_title="FE-COO Analytics Sim", layout="wide")

st.title("FE-COO Analytics Simulation")
st.caption("Mart tables built via SQL models + validated with unit tests + orchestrated with Prefect.")

st.subheader("Pipeline status (latest runs)")

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
LIMIT 10;
""").df()
con.close()

st.dataframe(df, use_container_width=True)

st.markdown("### Refresh data / rebuild marts")
st.code("python -m pipelines.build_mart_flow", language="bash")
st.code("python -c \"from pipelines.build_mart_flow import build_mart; build_mart(regenerate_raw=True)\"", language="bash")

st.info("In a real FE-COO setup, this flow would run on a schedule (e.g., hourly / daily) and alert on DQ failures.")
