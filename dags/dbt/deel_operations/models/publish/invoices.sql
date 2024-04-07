{{ config(
    materialized='table'

) }}


WITH invoices AS (
    SELECT 
    *
    FROM 
    {{ ref('invoices_transform')}}
)

SELECT
*
FROM 
invoices