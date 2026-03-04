{{
    config(
        materialized='incremental',
        unique_key='event_key',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'event_date', 
            'data_type': 'date'
        }
    )
}}

with source as (
    select * from {{ source('raw', 'lifecycle_events') }}

    {% if is_incremental() %}
        where event_date > (select max(event_date) from {{ this }})
    {% endif %}
),

renamed as (
     select
        -- IDs
        {{ generate_monthly_key('event_id', 'event_date') }} as event_key,
        cast(event_id as int64) as event_id, -- Original event_id from source, kept for reference but not unique
        cast(user_id as int64) as user_id,

        -- Event details
        event_type,
        properties,

        -- Timestamps
        event_date,
        cast(timestamp as timestamp) as event_at,

        -- Metadata
        current_timestamp() as _stg_loaded_at

    from source
)

select  * from renamed