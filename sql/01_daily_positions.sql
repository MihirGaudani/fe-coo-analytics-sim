CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE TABLE mart.daily_positions AS
    WITH signed_trades AS (
        SELECT trade_date::DATE AS date, strategy, ticker,
        CASE WHEN side = 'BUY'
            THEN quantity
            ELSE -quantity
        END AS signed_quantity
        FROM raw.trades
        ),
        daily_net_trades AS (
            SELECT date, strategy, ticker, SUM(signed_quantity) AS net_shares_change
            FROM signed_trades
            GROUP BY date, strategy, ticker
        ),
        grid AS (
            SELECT p.date::DATE AS date, s.strategy, sm.ticker
            FROM (SELECT DISTINCT date FROM raw.prices) AS p
            CROSS JOIN (SELECT DISTINCT strategy FROM raw.trades) AS s
            CROSS JOIN raw.security_master AS sm
        ), 
        grid_with_trades AS (
            SELECT g.date, g.strategy, g.ticker, COALESCE (dnt.net_shares_change, 0) AS net_shares_change
            FROM grid AS g 
            LEFT JOIN daily_net_trades dnt ON g.date = dnt.date AND  g.strategy = dnt.strategy AND g.ticker = dnt.ticker
        ),
        positions AS (
            SELECT date, strategy, ticker, SUM(net_shares_change) OVER (
                PARTITION BY strategy, ticker ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS shares
            FROM grid_with_trades
        )
        SELECT * FROM positions;