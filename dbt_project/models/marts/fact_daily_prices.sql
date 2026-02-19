{{
    config(
        materialized='incremental',
        unique_key='price_id',
        incremental_strategy='merge'
    )
}}

SELECT
    p.price_id,
    c.company_key,
    d.date_key,
    p.symbol,
    p.trade_date,
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.volume,
    (p.close_price - p.open_price) AS daily_price_change,
    ROUND(((p.close_price - p.open_price) / NULLIF(p.open_price, 0)) * 100, 4) AS daily_return_pct,
    (p.high_price - p.low_price) AS daily_range,
    p.loaded_at
FROM {{ ref('stg_daily_prices') }} p 
LEFT JOIN {{ ref('dim_company') }} c  
    ON p.symbol = c.symbol
LEFT JOIN {{ ref('dim_date') }} d 
    ON p.trade_date = d.date_day
{% if is_incremental() %}
WHERE p.loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}