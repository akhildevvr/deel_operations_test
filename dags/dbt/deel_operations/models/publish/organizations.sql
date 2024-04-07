{{ config(
    materialized='table'

) }}


WITH organizations AS (
    SELECT 
    *
    FROM 
    {{ ref('organizations_transform')}}
)

SELECT
*
FROM 
organizations