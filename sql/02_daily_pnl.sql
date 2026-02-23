CREATE OR REPLACE TABLE mart.daily_pnl AS
WITH px AS (
    SELECT date::DATE AS date, ticker, close,
    LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
    FROM raw.prices
),
pos AS (
    SELECT date, strategy, ticker, shares, 
    LAG(shares) OVER (PARTITION BY strategy, ticker ORDER BY date) AS prev_shares
    FROM mart.daily_positions
),
joined AS (
    SELECT p.date, p.strategy, p.ticker, p.prev_shares AS shares_held,
    x.close, x.prev_close, (x.close - x.prev_close) AS price_change
    FROM pos p
    JOIN px x ON p.date = x.date AND p.ticker = x.ticker
)
SELECT date, strategy, ticker, shares_held, close, prev_close, price_change,
COALESCE(shares_held, 0) * COALESCE(price_change, 0) AS pnl
FROM joined
WHERE prev_close IS NOT NULL;