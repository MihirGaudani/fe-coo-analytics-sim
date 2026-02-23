CREATE OR REPLACE TABLE mart.earnings_window_pnl AS
WITH e AS (
    SELECT ticker, earnings_date::DATE AS earnings_date
    FROM raw.earnings_calendar
),
p AS (
    SELECT date::DATE AS date, strategy, ticker, pnl
    FROM mart.daily_pnl
),
joined AS (
    SELECT p.strategy, p.ticker, e.earnings_date, p.date,
    DATE_DIFF('day', e.earnings_date, p.date) AS day_offset,
    p.pnl
    FROM p
    JOIN e ON p.ticker = e.ticker
    WHERE p.date BETWEEN e.earnings_date - INTERVAL 2 DAY AND e.earnings_date + INTERVAL 2 DAY
)
SELECT
  strategy,
  ticker,
  earnings_date,
  SUM(CASE WHEN day_offset = -2 THEN pnl ELSE 0 END) AS pnl_m2,
  SUM(CASE WHEN day_offset = -1 THEN pnl ELSE 0 END) AS pnl_m1,
  SUM(CASE WHEN day_offset =  0 THEN pnl ELSE 0 END) AS pnl_0,
  SUM(CASE WHEN day_offset =  1 THEN pnl ELSE 0 END) AS pnl_p1,
  SUM(CASE WHEN day_offset =  2 THEN pnl ELSE 0 END) AS pnl_p2,
  SUM(pnl) AS pnl_total_window
FROM joined
GROUP BY 1,2,3;