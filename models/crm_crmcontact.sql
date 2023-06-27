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
        COALESCE({{ var("table_prefix") }}_contact."Id", '') ||
        COALESCE({{ var("table_prefix") }}_contact."email", '') ||
        COALESCE({{ var("table_prefix") }}_contact."firstname", '') ||
        COALESCE({{ var("table_prefix") }}_contact."lastname", '') ||
        'contact' ||
        'salesforce_crm'
      )  as id,
      'salesforce_crm' as source,
      '{{ var("integration_id") }}'::uuid  as integration_id,
      _airbyte_raw_{{ var("table_prefix") }}_contact._airbyte_data as last_raw_data, 
      '{{ var("timestamp") }}' as sync_timestamp,
      {{ var("table_prefix") }}_contact."Id" as external_id,
      "{{ var("table_prefix") }}_contact".mailingcity as city,
      "{{ var("table_prefix") }}_contact".mailingpostalcode as zip,
      "{{ var("table_prefix") }}_contact".email,
      CONCAT(
        "{{ var("table_prefix") }}_contact".mobilephone,
        "{{ var("table_prefix") }}_contact".phone,
        "{{ var("table_prefix") }}_contact".homephone
      ) as phone,
      "{{ var("table_prefix") }}_contact".mailingstate as state,
      NULL as degree,
      "{{ var("table_prefix") }}_contact".salutation as gender,
      "{{ var("table_prefix") }}_contact".mailingstreet as address,
      NULL as company_id,
      "{{ var("table_prefix") }}_contact".mailingcountry as country,
      "{{ var("table_prefix") }}_contact".title as jobtitle,
      "{{ var("table_prefix") }}_contact".lastname as lastname,
      "{{ var("table_prefix") }}_contact".firstname as firstname,
      NULL as industry,
      "{{ var("table_prefix") }}_contact".department as department,
      "{{ var("table_prefix") }}_contact".birthdate as date_of_birth,
      "{{ var("table_prefix") }}_contact".cleanstatus as status,
      NULL as lifecycle,
      {{ var("table_prefix") }}_contact."Name" as fullname,
      "{{ var("table_prefix") }}_contact".photourl  as photo,
      "{{ var("table_prefix") }}_contact".isdeleted  as deleted,
      "{{ var("table_prefix") }}_contact".fax as fax,
      "{{ var("table_prefix") }}_contact".description as description,
      "{{ var("table_prefix") }}_contact".leadsource as crm_source,
      "{{ var("table_prefix") }}_contact".languages__c as languages,
      NULL as revenue
  FROM "{{ var("table_prefix") }}_contact"
  LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_contact
  ON _airbyte_raw_{{ var("table_prefix") }}_contact._airbyte_ab_id = "{{ var("table_prefix") }}_contact"._airbyte_ab_id
)
UNION
(
  SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      COALESCE({{ var("table_prefix") }}_lead."Id", '') ||
      COALESCE({{ var("table_prefix") }}_lead."email", '') ||
      COALESCE({{ var("table_prefix") }}_lead."firstname", '') ||
      COALESCE({{ var("table_prefix") }}_lead."lastname", '') ||
      'contact' ||
      'salesforce_crm'
    )  as id,
    'salesforce_crm' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_lead._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    {{ var("table_prefix") }}_lead."Id" as external_id,
    "{{ var("table_prefix") }}_lead".city as city,
    "{{ var("table_prefix") }}_lead".postalcode as zip,
    "{{ var("table_prefix") }}_lead".email,
    CONCAT(
      "{{ var("table_prefix") }}_lead".mobilephone,
      "{{ var("table_prefix") }}_lead".phone
    ) as phone,
    "{{ var("table_prefix") }}_lead"."State" as state,
    NULL as degree,
    "{{ var("table_prefix") }}_lead".salutation as gender,
    "{{ var("table_prefix") }}_lead".street as address,
    NULL as company_id,
    "{{ var("table_prefix") }}_lead".country as country,
    "{{ var("table_prefix") }}_lead".title as jobtitle,
    "{{ var("table_prefix") }}_lead".lastname as lastname,
    "{{ var("table_prefix") }}_lead".firstname as firstname,
    "{{ var("table_prefix") }}_lead".industry as industry,
    NULL as department,
    NULL as date_of_birth,
    "{{ var("table_prefix") }}_lead".status as status,
    NULL as lifecycle,
    {{ var("table_prefix") }}_lead."Name" as fullname,
    "{{ var("table_prefix") }}_lead".photourl  as photo,
    "{{ var("table_prefix") }}_lead".isdeleted  as deleted,
    "{{ var("table_prefix") }}_lead".fax as fax,
    "{{ var("table_prefix") }}_lead".description as description,
    "{{ var("table_prefix") }}_lead".leadsource as crm_source,
    NULL as languages,
    NULL as revenue
  FROM "{{ var("table_prefix") }}_lead"
  LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_lead
  ON _airbyte_raw_{{ var("table_prefix") }}_lead._airbyte_ab_id = "{{ var("table_prefix") }}_lead"._airbyte_ab_id
)