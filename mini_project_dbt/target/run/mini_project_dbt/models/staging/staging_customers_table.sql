
  
    

        create or replace transient table MINI_PROJECT_OUTPUT_DB.staging.staging_customers_table
         as
        (
WITH source AS (
    SELECT
        DISTINCT
        CAST(CUSTOMER_ID AS INT)        AS customer_id,
        CAST(CUSTOMERNAME AS STRING)    AS customer_name,
        COMPANY                         AS company
    FROM raw_data.public.CUSTOMERS_TABLE
    WHERE
        
    
        customer_id IS NOT NULL
         AND 
    
        customer_name IS NOT NULL
         AND 
    
        company IS NOT NULL
        
    

)
SELECT * FROM source
        );
      
  