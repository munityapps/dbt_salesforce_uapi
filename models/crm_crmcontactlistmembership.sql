{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

(
  SELECT 
      NOW() as created,
      NOW() as modified,
      md5(
        '{{ var("integration_id") }}' ||
        "Id" ||
        'contactlistmembership' ||
        'salesforce'
      )  as id,
      'salesforce' as source,
      '{{ var("integration_id") }}'::uuid  as integration_id,
      _airbyte_raw_{{ var("table_prefix") }}_campaignmember._airbyte_data as last_raw_data, 
      '{{ var("timestamp") }}' as sync_timestamp,
      "Id" as external_id,
      contactlist.id as contact_list_id,
      contact.id as contact_id
  FROM "{{ var("schema") }}"."{{ var("table_prefix") }}_campaignmember"
  LEFT JOIN "{{ var("schema") }}"._airbyte_raw_{{ var("table_prefix") }}_campaignmember
  ON _airbyte_raw_{{ var("table_prefix") }}_campaignmember._airbyte_ab_id = "{{ var("table_prefix") }}_campaignmember"._airbyte_ab_id
  LEFT JOIN {{ ref('crm_crmcontact') }} as contact
  ON contact.external_id::text = "{{ var("table_prefix") }}_campaignmember"."contactid" AND contact.integration_id = '{{ var("integration_id") }}'
  LEFT JOIN {{ ref('crm_crmcontactlist') }} as contactlist
  ON contactlist.external_id::text = "{{ var("table_prefix") }}_campaignmember"."campaignid" AND contactlist.integration_id = '{{ var("integration_id") }}'
  WHERE "{{ var("table_prefix") }}_campaignmember"."contactid" IS NOT NULL
) 
UNION
(
  SELECT 
      NOW() as created,
      NOW() as modified,
      md5(
        '{{ var("integration_id") }}' ||
        "Id" ||
        'contactlistmembership' ||
        'salesforce'
      )  as id,
      'salesforce' as source,
      '{{ var("integration_id") }}'::uuid  as integration_id,
      _airbyte_raw_{{ var("table_prefix") }}_campaignmember._airbyte_data as last_raw_data, 
      '{{ var("timestamp") }}' as sync_timestamp,
      "Id" as external_id,
      contactlist.id as contact_list_id,
      contact.id as contact_id
  FROM "{{ var("schema") }}"."{{ var("table_prefix") }}_campaignmember"
  LEFT JOIN "{{ var("schema") }}"._airbyte_raw_{{ var("table_prefix") }}_campaignmember
  ON _airbyte_raw_{{ var("table_prefix") }}_campaignmember._airbyte_ab_id = "{{ var("table_prefix") }}_campaignmember"._airbyte_ab_id
  LEFT JOIN {{ ref('crm_crmcontact') }} as contact
  ON contact.external_id::text = "{{ var("table_prefix") }}_campaignmember"."leadid" AND contact.integration_id = '{{ var("integration_id") }}'
  LEFT JOIN {{ ref('crm_crmcontactlist') }} as contactlist
  ON contactlist.external_id::text = "{{ var("table_prefix") }}_campaignmember"."campaignid" AND contactlist.integration_id = '{{ var("integration_id") }}'
  WHERE "{{ var("schema") }}"."{{ var("table_prefix") }}_campaignmember"."leadid" IS NOT NULL
)
