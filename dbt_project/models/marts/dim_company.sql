SELECT 
    {{ dbt_utils.generate_surrogate_key(['symbol']) }} AS company_key,
    symbol,
    company_name,
    sector,
    sub_industry,
    market_cap_category
FROM {{ ref('seed_companies') }}