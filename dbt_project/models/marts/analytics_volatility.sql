WITH daily_returns AS (
    SELECT
        f.symbol,
        c.sector,
        f.trade_date,
        f.close_price,
        f.daily_return_pct
    FROM {{ ref('fact_daily_prices') }} f
    LEFT JOIN {{ ref('dim_company') }} c ON f.company_key = c.company_key
),

rolling_vol AS (
    SELECT
        symbol,
        sector,
        trade_date,
        close_price,
        daily_return_pct,

        -- 30-day rolling volatility (annualized: multiply by sqrt(252 trading days))
        ROUND(
            STDDEV(daily_return_pct) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) * SQRT(252),
            4
        ) AS volatility_30d_annualized,

        -- Sector-level rolling volatility
        ROUND(
            STDDEV(daily_return_pct) OVER (
                PARTITION BY sector ORDER BY trade_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) * SQRT(252),
            4
        ) AS sector_volatility_30d_annualized

    FROM daily_returns
)

SELECT
    *,
    -- Flag high-volatility days (stock vol > 1.5x its sector)
    CASE
        WHEN volatility_30d_annualized > sector_volatility_30d_annualized * 1.5
        THEN TRUE ELSE FALSE
    END AS is_high_volatility
FROM rolling_vol
