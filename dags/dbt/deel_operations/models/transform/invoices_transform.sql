{{ config(
    materialized='view',
    alias = 'invoices'

) }}

-- Invoices fact table

WITH invoices AS (
    SELECT 
    *
    FROM {{ source('OPERATIONS_INGEST','INVOICES') }}
) 

SELECT 
    organization_id,
    status,
    SUM(amount) AS amount,
    SUM(payment_amount) AS payment_amount,
FROM invoices
GROUP BY 
    organization_id,
    status