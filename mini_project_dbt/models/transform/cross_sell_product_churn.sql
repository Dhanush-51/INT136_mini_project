{{
    config(
        tags = ["transform", "customer_revenue_ranking"]
    )
}}
WITH customer_product_sales AS (
    SELECT
        customer_id,
        product_id,
        COUNT(*) AS purchase_count,
        MAX(transaction_date) AS last_purchase_date
    FROM {{ ref('staging_transactions_table') }}  
    GROUP BY customer_id, product_id
),
cross_sell AS (
    SELECT
        customer_id,
        COUNT(DISTINCT product_id) AS cross_sell_products_purchased
    FROM customer_product_sales
    GROUP BY customer_id
),
previous_month AS (
    SELECT
        customer_id,
        product_id,
        last_purchase_date,
        DATE_TRUNC('month', last_purchase_date) - INTERVAL '1 month' AS previous_month_start,
        DATE_TRUNC('month', last_purchase_date) AS previous_month_end
    FROM customer_product_sales
),
recent_product_churn AS (
    SELECT
        pm.customer_id,
        pm.product_id
    FROM previous_month pm
    LEFT JOIN customer_product_sales cps ON pm.customer_id = cps.customer_id 
        AND pm.product_id = cps.product_id
        AND cps.last_purchase_date >= pm.previous_month_start
        AND cps.last_purchase_date < pm.previous_month_end
    WHERE cps.customer_id IS NULL 
),
product_churn AS (
    SELECT
        customer_id,
        COUNT(DISTINCT product_id) AS churned_products
    FROM recent_product_churn
    GROUP BY customer_id
)
SELECT
    cs.customer_id,
    cs.distinct_products_purchased,
    COALESCE(pc.churned_products, 0) AS churned_products
FROM cross_sell cs
LEFT JOIN product_churn pc ON cs.customer_id = pc.customer_id
ORDER BY cs.distinct_products_purchased DESC, churned_products DESC