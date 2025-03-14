{{
    config(
        tags = ["staging", "products"]
    )
}}
WITH source AS (
    SELECT
        DISTINCT
        PRODUCT_ID,
        PRODUCT_FAMILY,
        PRODUCT_SUB_FAMILY
    FROM {{ source('raw_data', 'PRODUCTS_TABLE') }}
    WHERE
        {{ not_null(['PRODUCT_ID', 'PRODUCT_FAMILY', 'PRODUCT_SUB_FAMILY']) }}
)
SELECT * FROM source