{{
    config(
        tags = ["staging", "customers"]
    )
}}
WITH source AS (
    SELECT
        DISTINCT
        CAST(CUSTOMER_ID AS INT)        AS customer_id,
        CAST(CUSTOMERNAME AS STRING)    AS customer_name,
        COMPANY                         AS company
    FROM {{ source('raw_data', 'CUSTOMERS_TABLE') }}
    WHERE
        {{ not_null(['customer_id', 'customer_name', 'company']) }}
)
SELECT * FROM source