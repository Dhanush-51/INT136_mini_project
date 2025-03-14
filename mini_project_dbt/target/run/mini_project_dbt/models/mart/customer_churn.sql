
  
    

        create or replace transient table MINI_PROJECT_OUTPUT_DB.mart.customer_churn
         as
        (WITH customer_activity AS (
    SELECT
        customer_id,
        MIN(transaction_date) AS first_purchase_date,
        MAX(transaction_date) AS last_purchase_date
    FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table
    GROUP BY customer_id
)

SELECT
    customer_id,
    first_purchase_date,
    last_purchase_date,
    CASE
        WHEN last_purchase_date < DATEADD('month', -6, CURRENT_DATE) THEN 'churned'
        WHEN first_purchase_date >= DATE_TRUNC('year', CURRENT_DATE) THEN 'new'
        ELSE 'active'
    END AS customer_status
FROM customer_activity
        );
      
  