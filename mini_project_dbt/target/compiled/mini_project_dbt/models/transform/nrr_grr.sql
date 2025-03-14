

WITH customer_first_purchase AS (
    SELECT
        customer_id,
        MIN(transaction_date) AS first_purchase_date
    FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table  
    GROUP BY customer_id
),
monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', t.transaction_date) AS month,
        t.customer_id,
        SUM(t.revenue) AS total_revenue,
        CASE 
            WHEN DATE_TRUNC('month', t.transaction_date) = DATE_TRUNC('month', cfp.first_purchase_date) THEN 'new' 
            ELSE 'existing' 
        END AS customer_type
    FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table t
    JOIN customer_first_purchase cfp ON t.customer_id = cfp.customer_id
    GROUP BY month, t.customer_id, cfp.first_purchase_date
),
starting_mrr AS (
    SELECT
        month,
        SUM(CASE WHEN customer_type = 'existing' THEN total_revenue ELSE 0 END) AS starting_mrr,
        SUM(CASE WHEN customer_type = 'new' THEN total_revenue ELSE 0 END) AS new_mrr
    FROM monthly_revenue
    WHERE month = (SELECT MIN(month) FROM monthly_revenue) 
    GROUP BY month
),
churned_mrr AS (
    SELECT
        month,
        SUM(CASE WHEN total_revenue < 0 THEN total_revenue ELSE 0 END) AS churned_mrr
    FROM monthly_revenue
    WHERE customer_type = 'existing' 
    GROUP BY month
),
expansion_mrr AS (
    SELECT
        month,
        SUM(CASE WHEN total_revenue > 0 THEN total_revenue ELSE 0 END) AS expansion_mrr
    FROM monthly_revenue
    WHERE customer_type = 'existing' 
    GROUP BY month
),
nrr_grr AS (
    SELECT
        m.month,
        sm.starting_mrr,
        COALESCE(cm.churned_mrr, 0) AS churned_mrr,
        COALESCE(em.expansion_mrr, 0) AS expansion_mrr,
        (sm.starting_mrr - COALESCE(cm.churned_mrr, 0)) AS grr,
        (sm.starting_mrr + COALESCE(em.expansion_mrr, 0) - COALESCE(cm.churned_mrr, 0)) AS nrr
    FROM (SELECT DISTINCT month FROM monthly_revenue) m
    LEFT JOIN starting_mrr sm ON m.month = sm.month
    LEFT JOIN churned_mrr cm ON m.month = cm.month
    LEFT JOIN expansion_mrr em ON m.month = em.month
)
SELECT
    n.month,
    n.starting_mrr,
    n.churned_mrr,
    n.expansion_mrr,
    (n.grr / NULLIF(n.starting_mrr, 0)) * 100 AS grr_percentage,
    (n.nrr / NULLIF(n.starting_mrr, 0)) * 100 AS nrr_percentage
FROM nrr_grr n
ORDER BY n.month