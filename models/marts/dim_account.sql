{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='account_id',
    merge_exclude_columns=['ACCOUNT_SK','src_created_at','dbt_loaded_at'],
    on_schema_change='sync_all_columns'
) }}

with stg as (select * from {{ ref('stg_account') }}),
state as (
  select state_code, state_name, region
  from {{ ref('state_codes') }}  -- seed
)
select
  {{ surrogate_key(['account_id']) }} as account_sk,
  s.account_id,
  s.account_number,
  s.account_name,
  s.segment,
  s.state_code,
  st.state_name,
  st.region,
  s.src_created_at,
  s.src_updated_at,
  s.dbt_loaded_at
from stg s
left join state st using (state_code)
{% if is_incremental() %}
where s.src_updated_at > (select coalesce(max(src_updated_at), '1900-01-01') from {{ this }})
{% endif %}