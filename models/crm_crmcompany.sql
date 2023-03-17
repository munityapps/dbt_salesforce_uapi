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
      "{{ var("table_prefix") }}_account"."Id" ||
      'company' ||
      'salesforce'
    )  as id,
    'salesforce' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_account._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_account"."Id" as external_id,
    "{{ var("table_prefix") }}_account"."Name" as name,
    "{{ var("table_prefix") }}_account"."Type" as type,
    "{{ var("table_prefix") }}_account"."accountsource" as company_source,
    "{{ var("table_prefix") }}_account"."active__c"::boolean as active,
    "{{ var("table_prefix") }}_account".annualrevenue as annual_revenue,
    "{{ var("table_prefix") }}_account".billingcity as billing_address_city,
    "{{ var("table_prefix") }}_account".billingcountry as billing_address_country,
    "{{ var("table_prefix") }}_account".billingpostalcode as billing_address_zip,
    "{{ var("table_prefix") }}_account".billingstate as billing_address_state,
    "{{ var("table_prefix") }}_account".billingstreet as billing_address_street,
    "{{ var("table_prefix") }}_account".shippingcity as shipping_address_city,
    "{{ var("table_prefix") }}_account".shippingcountry as shipping_address_country,
    "{{ var("table_prefix") }}_account".shippingpostalcode as shipping_address_zip,
    "{{ var("table_prefix") }}_account".shippingstate as shipping_address_state,
    "{{ var("table_prefix") }}_account".shippingstreet as shipping_address_street,
    "{{ var("table_prefix") }}_account".cleanstatus as status,
    "{{ var("table_prefix") }}_account".customerpriority__c as priority,
    "{{ var("table_prefix") }}_account".description as description,
    "{{ var("table_prefix") }}_account".fax as fax,
    "{{ var("table_prefix") }}_account".industry as industry,
    "{{ var("table_prefix") }}_account".isdeleted::boolean as deleted,
    "{{ var("table_prefix") }}_account".numberofemployees as number_of_employees,
    "{{ var("table_prefix") }}_account".phone as phone,
    "{{ var("table_prefix") }}_account".photourl as photo,
    "{{ var("table_prefix") }}_account".rating as rating,
    "{{ var("table_prefix") }}_account".website as website,
    NULL as lifecycle
FROM "{{ var("table_prefix") }}_account"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_account
ON _airbyte_raw_{{ var("table_prefix") }}_account._airbyte_ab_id = "{{ var("table_prefix") }}_account"._airbyte_ab_id
