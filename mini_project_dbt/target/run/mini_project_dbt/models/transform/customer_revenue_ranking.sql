
  
    

        create or replace transient table MINI_PROJECT_OUTPUT_DB.transform.customer_revenue_ranking
         as
        (
WITH transactions AS (
    SELECT * FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table
)
SELECT
    customer_id,
    SUM(revenue) AS total_revenue,
    RANK() OVER (ORDER BY SUM(revenue) DESC) AS customer_rank
FROM transactions
GROUP BY customer_id
        );
      
  