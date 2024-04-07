
{{ config(
    materialized='view',
    alias = 'organizations'

) }}


-- Organizations Dimension table
WITH organizations AS (
    SELECT 
    *
    FROM {{source('OPERATIONS_INGEST','ORGANIZATIONS')}}
),
invoices AS (
    SELECT
    *
    FROM {{source('OPERATIONS_INGEST','INVOICES')}}

)
SELECT 
    org.organization_id,
    MIN(org.first_payment_date) AS first_payment_date,
    MAX(org.last_payment_date) AS last_payment_date,
    org.legal_entity_country_code,
    org.count_total_contracts_active,
    org.created_date,
    COUNT(inv.invoice_id) AS num_invoices,
    COUNT(inv.transaction_id) AS num_transactions,
    DATEDIFF(day,current_date(), MIN(org.first_payment_date)) AS days_since_first_payment,
    DATEDIFF(day,current_date(), MIN(org.last_payment_date)) AS days_since_last_payment
FROM organizations org
LEFT JOIN 
    invoices inv 
    ON org.organization_id = inv.organization_id
GROUP BY 
    org.organization_id,
    org.legal_entity_country_code,
    org.count_total_contracts_active,
    org.created_date