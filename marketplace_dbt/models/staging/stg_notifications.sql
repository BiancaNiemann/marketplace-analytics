{{
    config(
        materialized='incremental',
        unique_key='notification_key',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'sent_date', 
            'data_type': 'date'
        }
    )
}}

with source as (
    select * from {{ source('raw', 'notifications') }}

    {% if is_incremental() %}
        where sent_date > (select max(sent_date) from {{ this }})
    {% endif %}
),

renamed as (
     select
        -- IDs
        {{ generate_monthly_key('notification_id', 'sent_date') }} as notification_key,
        cast(notification_id as int64) as notification_id, -- Original notification_id from source, kept for reference but not unique
        cast(user_id as int64) as user_id,

        -- Notification details
        type as notification_type,
        channel,

        -- Engagement
        cast(opened as boolean) as was_opened,

        -- Timestamps
        sent_date,
        cast(sent_ts as timestamp) as sent_at,
        
        -- Metadata
        current_timestamp() as _stg_loaded_at

    from source
)

select * from renamed
