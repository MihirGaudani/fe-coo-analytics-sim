from pathlib import Path
from models.db import get_conn

SQL_FILES = [
    "sql/01_daily_positions.sql",
    "sql/02_daily_pnl.sql",
    "sql/03_exposures.sql",
    "sql/04_liquidity.sql",
    "sql/05_earnings_window.sql",
]

def main() -> None:
    con = get_conn()
    for f in SQL_FILES:
        path = Path(f)
        if not path.exists():
            raise FileNotFoundError(f"Missing SQL file: {path.resolve()}")
        sql = path.read_text()
        con.execute(sql)
        print(f"Ran: {f}")
    con.close()
    print("Done. Models built in schema: mart")

if __name__ == "__main__":
    main()
