
WITH transactions AS (
    SELECT * FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table
)
SELECT
    customer_id,
    COUNT(DISTINCT PRODUCT_ID) AS product_churn_count_last_month
FROM transactions
WHERE transaction_date < DATEADD(month, -1, (SELECT MAX(transaction_date) FROM transactions))
GROUP BY customer_id
HAVING COUNT(DISTINCT PRODUCT_ID) = 0