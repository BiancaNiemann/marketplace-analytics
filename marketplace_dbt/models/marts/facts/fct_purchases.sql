{{
    config(
        materialized='incremental',
        unique_key='purchase_key',
        partition_by={
            'field': 'purchase_date', 
            'data_type': 'date'
        }
    )
}}

with purchases as (
    select * from {{ ref('stg_purchases') }}

    {% if is_incremental() %}
        where purchase_timestamp > (select max(purchase_timestamp) from {{ this }})
    {% endif %}
),

item as (
    select
        item_id,
        item_key,
        seller_id,
        title,
        category,
        condition,
        listed_date
    from {{ ref('stg_items') }}
)

select
    p.purchase_id,
    p.item_id,
    p.buyer_id as user_id,
    i.seller_id,

    --Timestamps
    p.purchase_timestamp,
    p.purchase_date,
    purchase_year,
    purchase_month, 
    purchase_quarter,
    purchase_year_month,
    purchase_day_of_week,

    --Financial metrics
    p.purchase_amount,
    p.platform_fee,
    p.seller_payout,
    p.payment_method,

    -- Product attributes
    i.title,
    i.category,
    i.condition,

    --Metadata
    current_timestamp() as dbt_updated_at

from purchases p
left join item i on p.item_key = i.item_key
