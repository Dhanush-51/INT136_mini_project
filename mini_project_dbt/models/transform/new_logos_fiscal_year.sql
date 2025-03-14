{{
    config(
        tags = ["transform", "customer_revenue_ranking"]
    )
}}

WITH customer_first_purchase AS (
    SELECT
        customer_id,
        MIN(transaction_date) AS first_purchase_date
    FROM {{ ref('staging_transactions_table') }}
    GROUP BY customer_id
),
fiscal_years AS (
    SELECT
        customer_id,
        first_purchase_date,
        CASE 
            WHEN EXTRACT(MONTH FROM first_purchase_date) >= 4 THEN EXTRACT(YEAR FROM first_purchase_date)
            ELSE EXTRACT(YEAR FROM first_purchase_date) - 1
        END AS fiscal_year
    FROM customer_first_purchase
)
SELECT
    fiscal_year,
    COUNT(customer_id) AS new_logos
FROM fiscal_years
GROUP BY fiscal_year
ORDER BY fiscal_year