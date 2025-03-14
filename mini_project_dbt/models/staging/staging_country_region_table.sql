{{
    config(
        tags = ["staging","country_region"]
    )
}}
WITH source AS (
    SELECT
        DISTINCT
        CAST(customer_id AS INT) AS customer_id
        , LOWER(country)         AS country
        , LOWER(region)          AS region
    FROM
        {{ source('raw_data', 'COUNTRY_REGION_TABLE') }}
    WHERE
        {{ not_null(['customer_id', 'country', 'region']) }}
)
SELECT
    *
FROM
    source