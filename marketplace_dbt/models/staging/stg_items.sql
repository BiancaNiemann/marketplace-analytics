{{
    config(
        materialized='incremental',
        unique_key='item_key',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'listed_date', 
            'data_type': 'date'
        }
    )
}}

with source as (
    select * from {{ source('raw', 'items') }}

    {% if is_incremental() %}
        where listed_date > (select max(listed_date) from {{ this }})
    {% endif %}
), 

renamed as (
    select
        -- IDs
        {{ generate_monthly_key('item_id', 'listed_date') }} as item_key,  -- Surrogate key for the item, combining item_id and listed_date to ensure uniqueness
        cast(item_id as int64) as item_id, -- Original item_id from source, kept for reference but not unique
        cast(seller_id as int64) as seller_id,

        -- Product Details
        title,
        category,
        cast(price as numeric) as price,
        currency,
        condition,

        -- Dates
        listed_date,
        cast(created_ts as timestamp) as listed_timestamp,

        -- Status
        status,
        case
            when status = 'sold' then listed_date + interval 7 day
            else null
        end as sold_date,

        -- Metadata
        current_timestamp() as _stg_loaded_at
        
    from source
)

select * from renamed