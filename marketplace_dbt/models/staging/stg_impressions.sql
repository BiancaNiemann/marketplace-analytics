{{
    config(
        materialized='incremental',
        unique_key='impression_id_unique',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'impression_date', 
            'data_type': 'date'
        },
        cluster_by=['user_id', 'item_id']
    )
}}

with source as (
    select * from {{ source('raw', 'impressions') }}

    {% if is_incremental() %}
        where impression_date > (select max(impression_date) from {{ this }})
    {% endif %}
),

renamed as (
     select
        -- IDs
        {{ dbt_utils.generate_surrogate_key(['impression_id', 'impression_date']) }} as impression_id_unique,
        cast(impression_id as int64) as impression_id,  -- Original impression_id from source, kept for reference but not unique
        cast(search_id as int64) as search_id,
        cast(user_id as int64) as user_id,
        cast(item_id as int64) as item_id,

        -- Context
        cast(position as int64) as position,

        -- Timestamps
        impression_date,
        cast(timestamp as timestamp) as impression_timestamp,

        -- Metadata
        current_timestamp() as _stg_loaded_at

    from source
)

select * from renamed
