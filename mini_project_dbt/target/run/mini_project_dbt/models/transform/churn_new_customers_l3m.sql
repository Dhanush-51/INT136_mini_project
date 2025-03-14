
  
    

        create or replace transient table MINI_PROJECT_OUTPUT_DB.transform.churn_new_customers_l3m
         as
        (WITH customer_transactions AS (
    SELECT
        customer_id,
        MIN(transaction_date) AS first_purchase_date,
        MAX(transaction_date) AS last_purchase_date
    FROM (
        SELECT
            customer_id,
            transaction_date
        FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table  -- Reference to the transactions staging table
    ) AS transactions
    GROUP BY customer_id
),
monthly_customers AS (
    SELECT
        DATE_TRUNC('month', first_purchase_date) AS month,
        COUNT(customer_id) AS new_customers
    FROM customer_transactions
    GROUP BY month
),
churned_customers AS (
    SELECT
        DATE_TRUNC('month', last_purchase_date) AS month,
        COUNT(customer_id) AS churned_customers
    FROM customer_transactions
    WHERE last_purchase_date < DATEADD(month, -3, CURRENT_DATE)  -- Assuming churn if no purchase in the last 3 months
    GROUP BY month
)
SELECT
    COALESCE(n.month, c.month) AS month,
    COALESCE(n.new_customers, 0) AS new_customers,
    COALESCE(c.churned_customers, 0) AS churned_customers
FROM monthly_customers n
FULL OUTER JOIN churned_customers c ON n.month = c.month
ORDER BY month
        );
      
  