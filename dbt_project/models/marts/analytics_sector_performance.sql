WITH daily AS (
    SELECT
        c.sector,
        f.trade_date,
        d.year,
        d.month,
        d.week_of_year,
        AVG(f.daily_return_pct) AS avg_sector_return,
        SUM(f.volume)           AS total_sector_volume,
        COUNT(*)                AS stock_count
    FROM {{ ref('fact_daily_prices') }} f
    LEFT JOIN {{ ref('dim_company') }} c ON f.company_key = c.company_key
    LEFT JOIN {{ ref('dim_date') }}    d ON f.date_key = d.date_key
    GROUP BY 1, 2, 3, 4, 5
),

with_cumulative AS (
    SELECT
        *,
        -- Cumulative return per sector (YTD)
        SUM(avg_sector_return) OVER (
            PARTITION BY sector, year
            ORDER BY trade_date
        ) AS ytd_cumulative_return,

        -- Rank sectors by daily performance
        RANK() OVER (
            PARTITION BY trade_date
            ORDER BY avg_sector_return DESC
        ) AS daily_sector_rank

    FROM daily
)

SELECT * FROM with_cumulative
