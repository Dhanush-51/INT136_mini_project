
  
    

        create or replace transient table MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table
         as
        (
WITH source AS (
    SELECT
        DISTINCT
        CAST(customer_id AS INT)        AS customer_id,
        PRODUCT_ID,
        TO_DATE(SUBSTR(payment_month, 1, 10), 'YYYY-DD-MM') AS transaction_date,
        CAST(REVENUE_TYPE AS INT)       AS revenue_type,
        CAST(REVENUE AS FLOAT)          AS revenue,
        CAST(QUANTITY AS INT)           AS quantity,
        COMPANIES
    FROM raw_data.public.TRANSACTIONS_TABLE
    WHERE
        
    
        customer_id IS NOT NULL
         AND 
    
        PRODUCT_ID IS NOT NULL
         AND 
    
        transaction_date IS NOT NULL
         AND 
    
        revenue_type IS NOT NULL
         AND 
    
        revenue IS NOT NULL
         AND 
    
        quantity IS NOT NULL
         AND 
    
        COMPANIES IS NOT NULL
        
    

)
SELECT * FROM source WHERE revenue_type = 1
        );
      
  