SELECT
    symbol,
    trade_date,
    close_price,
    volume,
    AVG(close_price) OVER(
        PARTITION BY symbol ORDER BY trade_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW 
    ) AS moving_7d_avg,

    AVG(close_price) OVER(
        PARTITION BY symbol ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_30d_avg,

    AVG(close_price) OVER(
        PARTITION BY symbol ORDER BY trade_date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS moving_90d_avg,
-- Signal: price above/below 30-day SMA
    CASE
        WHEN close_price > AVG(close_price) OVER(
        PARTITION BY symbol ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) THEN 'ABOVE'
        ELSE 'BELOW'
    END AS signal_30d,

    AVG(volume) OVER(
        PARTITION BY symbol ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS volume_30d_avg,

    ROUND(volume / NULLIF(AVG(volume) OVER(
        PARTITION BY symbol ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 0), 2) AS volume_ratio

FROM {{ ref('fact_daily_prices') }}