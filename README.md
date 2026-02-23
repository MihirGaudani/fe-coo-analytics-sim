## Live Demo
Streamlit App: [https://<your-app>.streamlit.app](https://mihirgaudani-fe-coo-analytics-sim-apphome-awyfn9.streamlit.app/)

# FE-COO Analytics Simulator (Schonfeld-style)

End-to-end simulation of an FE-COO analytics workflow:
- raw trade/price/liquidity/earnings inputs (synthetic)
- SQL mart models: daily positions, daily PnL, exposures, liquidity risk, earnings-window PnL
- Python analytics library + pytest unit tests
- Prefect flow orchestration + ops logging
- Streamlit multi-page dashboard consuming mart tables

## Quickstart (local)

```bash
git clone <REPO_URL>
cd fe-coo-analytics-sim
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
