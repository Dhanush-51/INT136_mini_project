1. Data Ingestion & Source Configuration
Define dbt Sources (sources.yml)
yaml
Copy
Edit
version: 2

sources:
  - name: raw_data
    database: raw_data
    schema: public
    tables:
      - name: CUSTOMERS_TABLE
        description: "Contains customer details including ID, name, company, and created date."
      - name: PRODUCTS_TABLE
        description: "Stores product-related data such as product ID, name, category, price, and stock availability."
      - name: TRANSACTIONS_TABLE
        description: "Records all transactions including transaction ID, customer ID, product ID, purchase date, and amount."
      - name: COUNTRY_REGION_TABLE
        description: "Maps country and region information with country codes, region names, and geographical details."
2. Staging Layer (Data Cleansing & Standardization)
The staging layer ensures clean, structured data.

Staging Customers (stg_customers.sql)
sql
Copy
Edit
WITH source AS (
    SELECT
        CUSTOMER_ID,
        UPPER(TRIM(CUSTOMERNAME)) AS CUSTOMER_NAME,
        UPPER(TRIM(COMPANY)) AS COMPANY_NAME
    FROM {{ source('raw_data', 'CUSTOMERS_TABLE') }}
)

SELECT * FROM source
Staging Products (stg_products.sql)
sql
Copy
Edit
WITH source AS (
    SELECT
        PRODUCT_ID,
        UPPER(TRIM(PRODUCT_NAME)) AS PRODUCT_NAME,
        CATEGORY,
        PRICE
    FROM {{ source('raw_data', 'PRODUCTS_TABLE') }}
)

SELECT * FROM source
Staging Transactions (stg_transactions.sql)
sql
Copy
Edit
WITH source AS (
    SELECT
        TRANSACTION_ID,
        CUSTOMER_ID,
        PRODUCT_ID,
        TRANSACTION_DATE,
        AMOUNT
    FROM {{ source('raw_data', 'TRANSACTIONS_TABLE') }}
)

SELECT * FROM source
Staging Country Region (stg_country_region.sql)
sql
Copy
Edit
WITH source AS (
    SELECT
        COUNTRY_CODE,
        COUNTRY_NAME,
        REGION
    FROM {{ source('raw_data', 'COUNTRY_REGION_TABLE') }}
)

SELECT * FROM source
3. Mart Layer (Transformations & KPI Computations)
Number of Churned & New Customers (kpi_customer_churn.sql)
sql
Copy
Edit
WITH customer_activity AS (
    SELECT
        customer_id,
        MIN(transaction_date) AS first_purchase_date,
        MAX(transaction_date) AS last_purchase_date
    FROM {{ ref('stg_transactions') }}
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
Customers with Highest Cross-Sell & Product Churn (kpi_cross_sell_product_churn.sql)
sql
Copy
Edit
WITH customer_products AS (
    SELECT
        customer_id,
        product_id,
        COUNT(transaction_id) AS purchase_count
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id, product_id
)

SELECT
    customer_id,
    COUNT(DISTINCT product_id) AS unique_products_purchased,
    SUM(purchase_count) AS total_purchases,
    CASE
        WHEN COUNT(DISTINCT product_id) > 3 THEN 'high cross-sell'
        WHEN COUNT(DISTINCT product_id) = 1 THEN 'high product churn'
        ELSE 'moderate'
    END AS cross_sell_churn_category
FROM customer_products
GROUP BY customer_id
ORDER BY total_purchases DESC
Net Revenue Retention (NRR) & Gross Revenue Retention (GRR) (kpi_nrr_grr.sql)
sql
Copy
Edit
WITH revenue_by_period AS (
    SELECT
        DATE_TRUNC('month', transaction_date) AS period,
        customer_id,
        SUM(amount) AS revenue
    FROM {{ ref('stg_transactions') }}
    GROUP BY 1, 2
)

SELECT
    period,
    SUM(revenue) AS total_revenue,
    SUM(CASE WHEN customer_id IN (SELECT customer_id FROM {{ ref('kpi_customer_churn') }} WHERE customer_status != 'churned') 
        THEN revenue ELSE 0 END) AS net_revenue_retention,
    SUM(revenue) AS gross_revenue_retention,
    (SUM(CASE WHEN customer_id NOT IN (SELECT customer_id FROM {{ ref('kpi_customer_churn') }} WHERE customer_status = 'churned') 
        THEN revenue ELSE 0 END) / NULLIF(SUM(revenue), 0)) * 100 AS nrr_percentage,
    (SUM(revenue) / NULLIF(SUM(revenue), 0)) * 100 AS grr_percentage
FROM revenue_by_period
GROUP BY 1
ORDER BY period
Revenue Lost Due to Contraction (kpi_revenue_contraction.sql)
sql
Copy
Edit
WITH revenue_growth AS (
    SELECT
        DATE_TRUNC('month', transaction_date) AS period,
        customer_id,
        SUM(amount) AS revenue
    FROM {{ ref('stg_transactions') }}
    GROUP BY 1, 2
)

SELECT
    period,
    SUM(revenue) AS current_revenue,
    LAG(SUM(revenue)) OVER (ORDER BY period) AS previous_revenue,
    COALESCE(LAG(SUM(revenue)) OVER (ORDER BY period) - SUM(revenue), 0) AS revenue_contraction
FROM revenue_growth
GROUP BY period
ORDER BY period
New Logos in Each Fiscal Year (kpi_new_logos.sql)
sql
Copy
Edit
SELECT
    DATE_TRUNC('year', first_purchase_date) AS fiscal_year,
    COUNT(DISTINCT customer_id) AS new_logos
FROM {{ ref('kpi_customer_churn') }}
WHERE customer_status = 'new'
GROUP BY fiscal_year
ORDER BY fiscal_year
Rank Products Based on Revenue (kpi_rank_products.sql)
sql
Copy
Edit
WITH product_sales AS (
    SELECT
        product_id,
        SUM(amount) AS total_revenue
    FROM {{ ref('stg_transactions') }}
    GROUP BY product_id
)

SELECT
    product_id,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS product_rank
FROM product_sales
Rank Customers Based on Revenue (kpi_rank_customers.sql)
sql
Copy
Edit
WITH customer_revenue AS (
    SELECT
        customer_id,
        SUM(amount) AS total_revenue
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
)

SELECT
    customer_id,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS customer_rank
FROM customer_revenue
