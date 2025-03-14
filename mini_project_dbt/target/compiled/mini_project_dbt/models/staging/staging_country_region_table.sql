
WITH source AS (
    SELECT
        DISTINCT
        CAST(customer_id AS INT) AS customer_id
        , LOWER(country)         AS country
        , LOWER(region)          AS region
    FROM
        raw_data.public.COUNTRY_REGION_TABLE
    WHERE
        
    
        customer_id IS NOT NULL
         AND 
    
        country IS NOT NULL
         AND 
    
        region IS NOT NULL
        
    

)
SELECT
    *
FROM
    source