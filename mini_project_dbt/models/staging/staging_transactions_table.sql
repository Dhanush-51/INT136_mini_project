{{
    config(
        tags = ["staging", "transactions"]
    )
}}
WITH source AS (
    SELECT
        DISTINCT
        CAST(customer_id AS INT)        AS customer_id,
        PRODUCT_ID,
        TO_DATE(SUBSTR(payment_month, 1, 10), 'YYYY-DD-MM') AS transaction_date,
        CAST(REVENUE_TYPE AS INT)       AS revenue_type,
        CAST(REVENUE AS FLOAT)          AS revenue,
        CAST(QUANTITY AS INT)           AS quantity,
        COMPANIES
    FROM {{ source('raw_data', 'TRANSACTIONS_TABLE') }}
    WHERE
        {{ not_null(['customer_id', 'PRODUCT_ID', 'transaction_date', 'revenue_type', 'revenue', 'quantity', 'COMPANIES']) }}
)
SELECT * FROM source WHERE revenue_type = 1