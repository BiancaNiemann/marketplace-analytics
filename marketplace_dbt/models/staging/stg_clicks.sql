{{
    config(
        materialized='incremental',
        unique_key='click_id_unique',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'click_date', 
            'data_type': 'date'
        },
        cluster_by=['user_id', 'item_id']
    )
}}

with source as (
    select * from {{ source('raw', 'clicks') }}

    {% if is_incremental() %}
        where click_date > (select max(click_date) from {{ this }})
    {% endif %}
),

renamed as (
     select
        -- IDs
        {{ dbt_utils.generate_surrogate_key(['click_id', 'click_date']) }} as click_id_unique,
        cast(click_id as int64) as click_id,  -- Original click_id from source, kept for reference but not unique
        cast(impression_id as int64) as impression_id,
        cast(search_id as int64) as search_id,
        cast(user_id as int64) as user_id,
        cast(item_id as int64) as item_id,

        -- Context
        cast(position as int64) as position,

        -- Timestamps
        click_date,
        cast(click_ts as timestamp) as click_timestamp,

        -- Metadata
        current_timestamp() as _stg_loaded_at

    from source
)

select * from renamed

