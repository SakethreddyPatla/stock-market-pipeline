WITH date_spine AS (
    SELECT DISTINCT trade_date AS date_day
    FROM {{ ref('stg_daily_prices') }}
),

dates AS (
    SELECT
        date_day,
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        MONTHNAME(date_day) AS month_name,
        WEEK(date_day) AS week_of_year,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYNAME(date_day) AS day_name,
        CASE 
            WHEN DAYOFWEEK(date_day) IN (0,6) THEN FALSE
            ELSE TRUE
        END AS is_weekday,
        date_day = LAST_DAY(date_day, 'month') AS is_month_end,
        date_day = LAST_DAY(date_day, 'quarter') AS is_quarter_end,
        date_day = LAST_DAY(date_day, 'year') AS is_year_end
    FROM date_spine
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
    *
FROM dates