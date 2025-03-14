

WITH customer_revenue AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', transaction_date) AS month,
        SUM(revenue) AS total_revenue
    FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table
    GROUP BY customer_id, month
),
revenue_change_lm AS (
    SELECT
        customer_id,
        month,
        total_revenue,
        LAG(total_revenue) OVER (PARTITION BY customer_id ORDER BY month) AS previous_revenue,
        (LAG(total_revenue) OVER (PARTITION BY customer_id ORDER BY month) - total_revenue) AS revenue_loss_lm
    FROM customer_revenue
),
revenue_change_l3m AS (
    SELECT
        customer_id,
        month AS month_l3m,
        total_revenue,
        LAG(total_revenue, 3) OVER (PARTITION BY customer_id ORDER BY month) AS previous_revenue,
        (LAG(total_revenue, 3) OVER (PARTITION BY customer_id ORDER BY month) - total_revenue) AS revenue_loss_l3m
    FROM customer_revenue
),
revenue_change_ltm AS (
    SELECT
        customer_id,
        month AS month_ltm,
        total_revenue,
        LAG(total_revenue, 12) OVER (PARTITION BY customer_id ORDER BY month) AS previous_revenue,
        (LAG(total_revenue, 12) OVER (PARTITION BY customer_id ORDER BY month) - total_revenue) AS revenue_loss_ltm
    FROM customer_revenue
)
SELECT
    lm.month,
    SUM(CASE WHEN lm.revenue_loss_lm > 0 THEN lm.revenue_loss_lm ELSE 0 END) AS total_revenue_lost,
    SUM(CASE WHEN l3m.revenue_loss_l3m > 0 THEN l3m.revenue_loss_l3m ELSE 0 END) AS total_revenue_lost_3_months,
    SUM(CASE WHEN ltm.revenue_loss_ltm > 0 THEN ltm.revenue_loss_ltm ELSE 0 END) AS total_revenue_lost_12_months
FROM revenue_change_lm lm
LEFT JOIN revenue_change_l3m l3m ON lm.customer_id = l3m.customer_id AND lm.month = l3m.month_l3m
LEFT JOIN revenue_change_ltm ltm ON lm.customer_id = ltm.customer_id AND lm.month = ltm.month_ltm
GROUP BY lm.month
ORDER BY lm.month