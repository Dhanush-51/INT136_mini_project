
  
    

        create or replace transient table MINI_PROJECT_OUTPUT_DB.staging.staging_country_region
         as
        (
WITH removing_null_rows AS (
    SELECT
        *
    FROM
        raw_data.public.COUNTRY_REGION_TABLE
    WHERE
        
    
        customer_id IS NOT NULL
         AND 
    
        country IS NOT NULL
         AND 
    
        region IS NOT NULL
        
    

),
changing_cases AS (
    SELECT
        DISTINCT
        CAST(customer_id AS INT) AS customer_id
        , LOWER(country)         AS country
        , LOWER(region)          AS region
    FROM
        removing_null_rows
)
SELECT
    *
FROM
    changing_cases
        );
      
  