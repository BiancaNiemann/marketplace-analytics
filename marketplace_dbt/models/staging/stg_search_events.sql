{{
    config(
        materialized='incremental',
        unique_key='search_key',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'search_date', 
            'data_type': 'date'
        },
        cluster_by=['user_id']
    )
}}

with source as (
    select * from {{ source('raw', 'search_events') }}

    {% if is_incremental() %}
        where search_date > (select max(search_date) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- IDs
        {{ generate_monthly_key('search_id', 'search_date') }} as search_key,
        cast(search_id as int64) as search_id, -- Original search_id from source, kept for reference but not unique
        cast(user_id as int64) as user_id,

        -- Search details
        query as search_query,
        lower(trim(query)) as query_normalized,

        -- Timestamps
        search_date,
        cast(timestamp as timestamp) as searched_at,

        -- Metadata
        current_timestamp() as _stg_loaded_at
        
    from source
)

select * from renamed