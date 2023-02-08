{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT "{{ var("table_prefix") }}_users".id_employe as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      COALESCE("{{ var("table_prefix") }}_users".id_employe, '') ||
      COALESCE("{{ var("table_prefix") }}_users".nom, '') ||
      COALESCE("{{ var("table_prefix") }}_users".prenom, '') ||
      COALESCE("{{ var("table_prefix") }}_users".email, '') ||
      COALESCE("{{ var("table_prefix") }}_users".date_naissance, '') ||
      COALESCE("{{ var("table_prefix") }}_users".date_debut_contrat, '') ||
      'user' ||
      'comuto_hris'
    )  as id,
    'comuto_hris' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_users._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_users".email as email,
    "{{ var("table_prefix") }}_users".prenom as firstname,
    "{{ var("table_prefix") }}_users".date_naissance as birth_date,
    "{{ var("table_prefix") }}_users".nom as lastname,
    "{{ var("table_prefix") }}_users".telephone as phone_number,
    "{{ var("table_prefix") }}_users".entite_legale as legal_entity_name,
    "{{ var("table_prefix") }}_users".email_manager as manager_email,
    "{{ var("table_prefix") }}_users".intitule_de_poste as job_title,
    "{{ var("table_prefix") }}_users".equipe as team,
    "{{ var("table_prefix") }}_users".pays as country,
    "{{ var("table_prefix") }}_users".date_debut_contrat as contract_start_date,
    NULL as contract_end_date,
    NULL as language,
    "{{ var("table_prefix") }}_users".genre as gender,
    "{{ var("table_prefix") }}_users".direction as business_unit,
    "{{ var("table_prefix") }}_users".id_employe as employee_number,
    "{{ var("table_prefix") }}_users".type_contrat as contract_type,
    NULL as socio_professional_category,
    "{{ var("table_prefix") }}_users".statut as state
FROM "{{ var("table_prefix") }}_users"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_users
ON _airbyte_raw_{{ var("table_prefix") }}_users._airbyte_ab_id = "{{ var("table_prefix") }}_users"._airbyte_ab_id
