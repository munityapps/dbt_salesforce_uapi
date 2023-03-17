{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_opportunity"."Id" ||
      'company' ||
      'salesforce'
    )  as id,
    'salesforce' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_opportunity._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_opportunity"."Id" as external_id,
    "{{ var("table_prefix") }}_opportunity"."Name" as name,
    company.id as company_id,
    contact.id as contact_id,
    NULL::boolean as archived,
    "{{ var("table_prefix") }}_opportunity".amount as amount,
    NULL as closed_reason,
    "{{ var("table_prefix") }}_opportunity".closedate as closed_date,
    "{{ var("table_prefix") }}_opportunity".stagename as stage,
    "{{ var("table_prefix") }}_opportunity"."Type" as type,
    "{{ var("table_prefix") }}_opportunity"."description" as description,
    "{{ var("table_prefix") }}_opportunity".leadsource as deal_source,
    owner.id as created_by,
    "{{ var("table_prefix") }}_opportunity".createddate as created_at,
    ("{{ var("table_prefix") }}_opportunity".isclosed)::boolean as is_closed,
    ("{{ var("table_prefix") }}_opportunity".iswon)::boolean as is_won,
    NULL as mrr,
    NULL as priority
FROM "{{ var("table_prefix") }}_opportunity"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_opportunity
ON _airbyte_raw_{{ var("table_prefix") }}_opportunity._airbyte_ab_id = "{{ var("table_prefix") }}_opportunity"._airbyte_ab_id
LEFT JOIN crm_crmcontact as contact
ON contact.external_id = ("{{ var("table_prefix") }}_opportunity".contactid)::text
LEFT JOIN crm_crmcompany as company
ON company.external_id = ("{{ var("table_prefix") }}_opportunity".accountid)::text
LEFT JOIN crm_crmuser as owner
ON owner.external_id = ("{{ var("table_prefix") }}_opportunity".createdbyid)::text