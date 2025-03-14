
  
    

        create or replace transient table MINI_PROJECT_OUTPUT_DB.transform.product_revenue_ranking
         as
        (
WITH transactions AS (
    SELECT * FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table
)
SELECT
    PRODUCT_ID,
    SUM(revenue) AS total_revenue,
    RANK() OVER (ORDER BY SUM(revenue) DESC) AS product_rank
FROM transactions
GROUP BY PRODUCT_ID
        );
      
  