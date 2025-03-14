
  
    

        create or replace transient table MINI_PROJECT_OUTPUT_DB.staging.staging_products_table
         as
        (
WITH source AS (
    SELECT
        DISTINCT
        PRODUCT_ID,
        PRODUCT_FAMILY,
        PRODUCT_SUB_FAMILY
    FROM raw_data.public.PRODUCTS_TABLE
    WHERE
        
    
        PRODUCT_ID IS NOT NULL
         AND 
    
        PRODUCT_FAMILY IS NOT NULL
         AND 
    
        PRODUCT_SUB_FAMILY IS NOT NULL
        
    

)
SELECT * FROM source
        );
      
  