{{
    config(
        materialized='incremental',
        unique_key='purchase_id_unique',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'purchase_date', 'data_type': 'date'
        },
        cluster_by=['buyer_id', 'seller_id']
    )
}}

with source as (
    select * from {{ source('raw', 'purchases') }}

    {% if is_incremental() %}
    -- Only process new purchases since last run
        where purchase_date > (select max(purchase_date) from {{ this }})
    {% endif %}
),

renamed as (
     select
        -- IDs
        {{ dbt_utils.generate_surrogate_key(['purchase_id', 'purchase_date']) }} as purchase_id_unique,
        cast(purchase_id as int64) as purchase_id, -- Original purchase_id from source, kept for reference but not unique
        cast(buyer_id as int64) as buyer_id,
        cast(seller_id as int64) as seller_id,
        cast(item_id as int64) as item_id,

        -- Amounts
        cast(purchase_amount as numeric) as purchase_amount,
        cast(platform_fee as numeric) as platform_fee,
        cast((purchase_amount - platform_fee) as numeric) as seller_payout,

        -- Payment
        payment_method,
        currency,

        -- Timestamps
        purchase_date,
        cast(purchase_timestamp as timestamp) as purchase_timestamp,

        -- Date parts for analysis
        extract(year from purchase_date) as purchase_year,
        extract(month from purchase_date) as purchase_month,
        extract(quarter from purchase_date) as purchase_quarter,
        format_date('%Y-%m', purchase_date) as purchase_year_month,
        extract(dayofweek from purchase_date) as purchase_day_of_week,

        -- Metadata
        current_timestamp() as _stg_loaded_at

    from source
)

select * from renamed