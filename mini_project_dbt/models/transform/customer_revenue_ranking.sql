{{
    config(
        tags = ["transform", "customer_revenue_ranking"]
    )
}}
WITH transactions AS (
    SELECT * FROM {{ ref('staging_transactions_table') }}
)
SELECT
    customer_id,
    SUM(revenue) AS total_revenue,
    RANK() OVER (ORDER BY SUM(revenue) DESC) AS customer_rank
FROM transactions
GROUP BY customer_id