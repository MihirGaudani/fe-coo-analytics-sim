CREATE OR REPLACE TABLE mart.daily_liquidity AS
WITH px AS (
    SELECT date::DATE AS date, ticker, close
    FROM raw.prices
),
pos AS (
    SELECT date::DATE AS date, strategy, ticker, shares
    FROM mart.daily_positions
),
liq AS (
    SELECT ticker, adv_shares
    FROM raw.liquidity
), joined AS (
    SELECT p.date, p.strategy, p.ticker, p.shares, l.adv_shares, x.close,
    ABS(p.shares) / NULLIF(l.adv_shares, 0) AS days_to_liquidate
    FROM pos p
    JOIN liq l ON p.ticker = l.ticker
    JOIN px x ON p.date = x.date AND p.ticker = x.ticker
)
SELECT *,
    CASE WHEN days_to_liquidate > 3 THEN 1
     ELSE 0
    END AS illiquid_flag
FROM joined;
