# pipelines/build_mart_flow.py
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
import time
import traceback

from prefect import flow, task
from prefect.logging import get_run_logger

from models.db import get_conn  # canonical DB connector you already use


SQL_FILES = [
    "sql/01_daily_positions.sql",
    "sql/02_daily_pnl.sql",
    "sql/03_exposures.sql",
    "sql/04_liquidity.sql",
    "sql/05_earnings_window.sql",
]


@task(retries=0)
def ensure_ops_schema_and_table() -> None:
    con = get_conn()
    con.execute("CREATE SCHEMA IF NOT EXISTS ops;")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
            run_id VARCHAR,
            run_ts TIMESTAMP,
            status VARCHAR,
            regenerated_raw BOOLEAN,
            models_ran VARCHAR,
            duration_seconds DOUBLE,
            error_message VARCHAR
        );
        """
    )
    con.close()


@task(retries=0)
def regenerate_raw_data() -> None:
    """
    Calls your existing generator. Assumes it writes raw.* tables into the same DuckDB.
    """
    # Run as module to avoid import path issues
    import runpy

    runpy.run_module("models.generate_data", run_name="__main__")


@task(retries=0)
def run_sql_models(sql_files: list[str] = SQL_FILES) -> list[str]:
    logger = get_run_logger()
    con = get_conn()

    ran = []
    for f in sql_files:
        path = Path(f)
        if not path.exists():
            con.close()
            raise FileNotFoundError(f"Missing SQL file: {path.resolve()}")
        sql = path.read_text()
        t0 = time.time()
        con.execute(sql)
        dt = time.time() - t0
        logger.info(f"Ran: {f} ({dt:.3f}s)")
        ran.append(f)

    con.close()
    return ran


@task(retries=0)
def run_dq_checks() -> dict:
    """
    Runs a few core data-quality checks. Return structured results.
    """
    from fe_coo_analytics.validate import check_table_exists, check_row_count, check_unique_key

    checks = []

    # existence + min rows
    for t in [
        "daily_positions",
        "daily_pnl",
        "daily_exposures",
        "daily_liquidity",
        "earnings_window_pnl",
    ]:
        checks.append(check_table_exists("mart", t))
        checks.append(check_row_count("mart", t, min_rows=1))

    # uniqueness invariants
    checks.append(check_unique_key("mart", "daily_positions", ["date", "strategy", "ticker"]))

    passed = all(c.passed for c in checks)

    return {
        "passed": passed,
        "checks": [asdict(c) for c in checks],
    }


@task(retries=0)
def log_run(
    run_id: str,
    status: str,
    regenerated_raw: bool,
    models_ran: list[str],
    duration_seconds: float,
    error_message: str | None,
) -> None:
    con = get_conn()
    con.execute(
        """
        INSERT INTO ops.pipeline_runs
        (run_id, run_ts, status, regenerated_raw, models_ran, duration_seconds, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        [
            run_id,
            datetime.now(timezone.utc),
            status,
            regenerated_raw,
            ",".join(models_ran),
            duration_seconds,
            error_message,
        ],
    )
    con.close()


@flow(name="build-mart")
def build_mart(regenerate_raw: bool = False) -> dict:
    """
    Orchestrates: (optional) regenerate raw -> run SQL models -> run dq checks -> log run.
    """
    logger = get_run_logger()
    run_id = f"build-mart-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    t0 = time.time()

    ensure_ops_schema_and_table()

    models_ran: list[str] = []
    try:
        if regenerate_raw:
            logger.info("Regenerating raw tables...")
            regenerate_raw_data()

        logger.info("Running SQL models...")
        models_ran = run_sql_models()

        logger.info("Running DQ checks...")
        dq = run_dq_checks()

        if not dq["passed"]:
            raise RuntimeError(f"DQ checks failed: {dq}")

        status = "success"
        err = None
        result = {"run_id": run_id, "status": status, "dq": dq, "models_ran": models_ran}

    except Exception as e:
        status = "failed"
        err = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        result = {"run_id": run_id, "status": status, "error": str(e), "models_ran": models_ran}
        logger.error(err)

    duration = time.time() - t0
    log_run(run_id, status, regenerate_raw, models_ran, duration, err)

    return result


if __name__ == "__main__":
    build_mart(regenerate_raw=False)
