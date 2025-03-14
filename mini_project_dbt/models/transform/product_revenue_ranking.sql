{{
    config(
        tags = ["transform", "product_revenue_ranking"]
    )
}}
WITH transactions AS (
    SELECT * FROM {{ ref('staging_transactions_table') }}
)
SELECT
    PRODUCT_ID,
    SUM(revenue) AS total_revenue,
    RANK() OVER (ORDER BY SUM(revenue) DESC) AS product_rank
FROM transactions
GROUP BY PRODUCT_ID