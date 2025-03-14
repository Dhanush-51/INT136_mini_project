
  
    

        create or replace transient table MINI_PROJECT_OUTPUT_DB.transform.churned_customer_last_three
         as
        (
 
WITH first_last_purchase AS (
    SELECT
        t.customer_id,
        c.customer_name,
        MAX(transaction_date) AS last_purchase_date
    FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table  AS t
    LEFT JOIN MINI_PROJECT_OUTPUT_DB.staging.staging_customers_table AS c
    ON c.customer_id = t.customer_id
    GROUP BY t.customer_id, c.customer_name
),
max_min_overall AS(
    SELECT
        MAX(transaction_date) AS max_date
    FROM MINI_PROJECT_OUTPUT_DB.staging.staging_transactions_table
),
churned_customers AS (
    SELECT
        flp.customer_id,
        flp.customer_name,
        flp.last_purchase_date,
        mmo.max_date
    FROM first_last_purchase    AS flp
    CROSS JOIN max_min_overall  AS mmo
    WHERE flp.last_purchase_date < DATEADD(month, -3, mmo.max_date)
)
SELECT * FROM churned_customers
        );
      
  