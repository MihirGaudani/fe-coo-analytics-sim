from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import random
import string
import numpy as np
import pandas as pd

from models.db import get_conn


@dataclass(frozen=True)
class Config:
    seed: int = 42
    n_tickers: int = 40
    n_days: int = 90          # ~3 months of business days
    start_date: str = "2025-10-01"
    strategies: tuple[str, ...] = ("CORE", "TMT", "HEALTH")
    n_trades: int = 2500
    max_shares_per_trade: int = 800
    adv_min: int = 200_000
    adv_max: int = 5_000_000


SECTORS = [
    "Technology", "Healthcare", "Financials", "Consumer", "Industrials",
    "Energy", "Materials", "Utilities", "Real Estate", "Communications"
]

COUNTRIES = ["US", "UK", "DE", "FR", "JP", "CA"]


def make_tickers(n: int, seed: int) -> list[str]:
    random.seed(seed)
    tickers = set()
    while len(tickers) < n:
        t = "".join(random.choices(string.ascii_uppercase, k=3))
        tickers.add(t)
    return sorted(tickers)


def trading_days(start_date: str, n_days: int) -> pd.DatetimeIndex:
    start = pd.Timestamp(start_date)
    return pd.bdate_range(start=start, periods=n_days)


def simulate_prices(days: pd.DatetimeIndex, tickers: list[str], seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    out = []
    for t in tickers:
        p0 = rng.uniform(20, 250)
        vol = rng.uniform(0.008, 0.03)
        rets = rng.normal(loc=0.0002, scale=vol, size=len(days))
        prices = p0 * np.exp(np.cumsum(rets))
        out.append(pd.DataFrame({"date": days, "ticker": t, "close": prices.round(2)}))
    return pd.concat(out, ignore_index=True)


def security_master(tickers: list[str], seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 1)
    sectors = rng.choice(SECTORS, size=len(tickers), replace=True)
    countries = rng.choice(COUNTRIES, size=len(tickers), replace=True, p=[0.72, 0.06, 0.06, 0.06, 0.06, 0.04])

    def currency(c: str) -> str:
        if c == "US":
            return "USD"
        if c in ("DE", "FR"):
            return "EUR"
        if c == "UK":
            return "GBP"
        if c == "JP":
            return "JPY"
        return "CAD"

    return pd.DataFrame({
        "ticker": tickers,
        "sector": sectors,
        "country": countries,
        "currency": [currency(c) for c in countries],
    })


def liquidity_table(tickers: list[str], seed: int, adv_min: int, adv_max: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 2)
    adv = rng.integers(low=adv_min, high=adv_max, size=len(tickers))
    return pd.DataFrame({"ticker": tickers, "adv_shares": adv})


def earnings_calendar(days: pd.DatetimeIndex, tickers: list[str], seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 3)
    idx_min = max(5, int(len(days) * 0.15))
    idx_max = max(idx_min + 1, int(len(days) * 0.85))
    earnings_idx = rng.integers(low=idx_min, high=idx_max, size=len(tickers))
    earnings_dates = [days[i] for i in earnings_idx]
    return pd.DataFrame({"ticker": tickers, "earnings_date": pd.to_datetime(earnings_dates)})

def generate_trades(
    days: pd.DatetimeIndex,
    prices: pd.DataFrame,
    tickers: list[str],
    strategies: tuple[str, ...],
    n_trades: int,
    max_shares: int,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 4)

    # pick random business days
    day_choices = rng.choice(days.values, size=n_trades, replace=True)

    # random minute during market-ish hours (09:35 to 15:55)
    minute_of_day = rng.integers(low=9 * 60 + 35, high=15 * 60 + 55, size=n_trades)

    # IMPORTANT: convert numpy datetime64 safely -> pandas Timestamp, normalize to midnight
    timestamps = [
        pd.Timestamp(d).normalize() + pd.Timedelta(minutes=int(m))
        for d, m in zip(day_choices, minute_of_day)
    ]

    tick = rng.choice(tickers, size=n_trades, replace=True)
    strat = rng.choice(list(strategies), size=n_trades, replace=True)

    side = rng.choice(["BUY", "SELL"], size=n_trades, replace=True, p=[0.52, 0.48])
    qty = rng.integers(low=10, high=max_shares, size=n_trades)

    trade_date = pd.to_datetime([ts.date() for ts in timestamps])

    # join to the close price for that day to anchor trade price
    prices_key = prices.rename(columns={"date": "trade_date"})[["trade_date", "ticker", "close"]]
    base = pd.DataFrame({"trade_date": trade_date, "ticker": tick}).merge(
        prices_key, on=["trade_date", "ticker"], how="left"
    )

    # small noise around close
    noise = rng.normal(loc=0.0, scale=0.0025, size=n_trades)  # ~0.25%
    trade_px = (base["close"].values * (1 + noise)).round(2)

    df = pd.DataFrame(
        {
            "trade_id": np.arange(1, n_trades + 1),
            "timestamp": pd.to_datetime(timestamps),
            "trade_date": trade_date,
            "strategy": strat,
            "ticker": tick,
            "side": side,
            "quantity": qty,
            "price": trade_px,
        }
    )

    # add a few big trades (realistic spikes)
    big_idx = rng.choice(df.index, size=max(8, n_trades // 300), replace=False)
    df.loc[big_idx, "quantity"] = df.loc[big_idx, "quantity"] * 6

    return df




def write_csvs(out_dir: Path, **tables: pd.DataFrame) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(out_dir / f"{name}.csv", index=False)


def load_to_duckdb(**tables: pd.DataFrame) -> None:
    con = get_conn()
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    for name, df in tables.items():
        con.execute(f"DROP TABLE IF EXISTS raw.{name};")
        con.register("df_tmp", df)
        con.execute(f"CREATE TABLE raw.{name} AS SELECT * FROM df_tmp;")
        con.unregister("df_tmp")
    con.close()


def main() -> None:
    cfg = Config()
    days = trading_days(cfg.start_date, cfg.n_days)
    tickers = make_tickers(cfg.n_tickers, cfg.seed)

    prices = simulate_prices(days, tickers, cfg.seed)
    sec = security_master(tickers, cfg.seed)
    liq = liquidity_table(tickers, cfg.seed, cfg.adv_min, cfg.adv_max)
    earn = earnings_calendar(days, tickers, cfg.seed)
    trades = generate_trades(days, prices, tickers, cfg.strategies, cfg.n_trades, cfg.max_shares_per_trade, cfg.seed)

    write_csvs(Path("data/raw"),
               prices=prices,
               security_master=sec,
               liquidity=liq,
               earnings_calendar=earn,
               trades=trades)

    load_to_duckdb(prices=prices,
                   security_master=sec,
                   liquidity=liq,
                   earnings_calendar=earn,
                   trades=trades)

    con = get_conn(read_only=True)
    print("Loaded raw tables (schema raw):")
    for t in ["prices", "security_master", "liquidity", "earnings_calendar", "trades"]:
        n = con.execute(f"SELECT COUNT(*) FROM raw.{t}").fetchone()[0]
        print(f"  raw.{t}: {n:,} rows")
    dr = con.execute("SELECT MIN(date), MAX(date) FROM raw.prices").fetchone()
    print(f"Prices date range: {dr[0]} to {dr[1]}")
    con.close()


if __name__ == "__main__":
    main()
