WITH raw_data AS (
    SELECT 
        symbol,
        ingestion_date,
        api_response
    FROM {{ source('raw', 'daily_prices_raw') }}
),

flattened AS (
    SELECT
        r.symbol,
        r.ingestion_date,
        f.key::DATE AS trade_date,
        f.value:"1. open"::DECIMAL(18,4) AS open_price,
        f.value:"2. high"::DECIMAL(18,4) AS high_price,
        f.value:"3. low"::DECIMAL(18,4) AS low_price,
        f.value:"4. close"::DECIMAL(18,4) AS close_price,
        f.value:"5. volume"::BIGINT AS volume
    FROM raw_data r, 
    LATERAL FLATTEN(input => r.api_response:"Time Series (Daily)") f
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY symbol, trade_date ORDER BY ingestion_date DESC) AS row_num
    FROM flattened    
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['symbol', 'trade_date']) }} AS price_id,
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    ingestion_date AS loaded_at
FROM deduplicated
WHERE row_num = 1