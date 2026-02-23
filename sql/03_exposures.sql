CREATE OR REPLACE TABLE mart.daily_exposures AS
WITH px AS (
    SELECT date::DATE AS date, ticker, close
    FROM raw.prices
),
pos AS (
    SELECT date, strategy, ticker, shares
    FROM mart.daily_positions
),
 mv AS(
    SELECT p.date, p.strategy, p.ticker, p.shares, x.close,
    (p.shares * x.close) AS market_value
    FROM pos p
    JOIN px x ON p.date = x.date AND p.ticker = x.ticker
 )
 SELECT date, strategy, SUM(ABS(market_value)) AS gross_exposure,
 SUM(market_value) AS net_exposure
 FROM mv 
 GROUP BY date, strategy;