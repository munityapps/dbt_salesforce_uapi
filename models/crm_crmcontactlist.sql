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
      "{{ var("table_prefix") }}_campaign"."Id" ||
      'contactlist' ||
      'salesforce'
    )  as id,
    'salesforce' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_campaign._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_campaign"."Id" as external_id,
    "{{ var("table_prefix") }}_campaign"."Name" as name,
    "{{ var("table_prefix") }}_campaign"."Type" as type,
    NULL::boolean as deleted,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_campaign"."Id" ||
      'campaign' ||
      'salesforce'
    )  as campaign_id,
    numberofcontacts as size

FROM "{{ var("schema") }}"."{{ var("table_prefix") }}_campaign"
LEFT JOIN "{{ var("schema") }}"._airbyte_raw_{{ var("table_prefix") }}_campaign
ON _airbyte_raw_{{ var("table_prefix") }}_campaign._airbyte_ab_id = "{{ var("table_prefix") }}_campaign"._airbyte_ab_id
