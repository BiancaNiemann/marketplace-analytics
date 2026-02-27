{{
    config(
        materialized='incremental',
        unique_key='user_id',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'signup_date', 'data_type': 'date'
        }
    )
}}

with source as (
    select * from {{ source('raw', 'users') }}

    {% if is_incremental() %}
        where signup_date > (select max(signup_date) from {{ this }})
    {% endif %}
),

renamed as (
    select 
        -- IDs
        cast(user_id as int64) as user_id,
        email,

        -- Dates
        signup_date,
        cast(signup_ts as timestamp) as signup_at,

        -- Demographics
        country,
        city,
        age_group,

        -- User Type
        cast(is_seller as boolean) as is_seller,
        account_type,

        -- Aquitsition
        signup_channel,
        device_type,

        -- Preferences
        cast(marketing_opt_in as boolean) as marketing_opt_in,
        cast(is_verified as boolean) as is_verified,
        status,

        -- Metadata
        current_timestamp() as _stg_loaded_at
    from source
)

select * from renamed
