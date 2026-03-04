{{
    config(
        materialized='incremental',
        unique_key=['item_key', 'event_date'],
        partition_by={
            'field': 'event_date', 
            'data_type': 'date'
        }
    )
}}

with impressions as (
    select
        item_key,
        impression_date as event_date,
        count(*) as impressions,
        count(distinct user_id) as unique_viewers
    from {{ ref('stg_impressions') }}
    {%- if is_incremental() %}
        where impression_date > (select max(event_date) from {{ this }})
    {%- endif %}
    group by 1, 2
),

clicks as (
    select
        item_key,
        click_date as event_date,
        count(*) as clicks,
        count(distinct user_id) as unique_clickers
    from {{ ref('stg_clicks') }}
    {% if is_incremental() %}
    where click_date > (select max(event_date) from {{ this }})
    {% endif %}
    group by 1, 2
),

items as (
    select 
    item_key,
    seller_id,
    category,
    title,
    price,
    listed_date as event_date
    from {{ ref('stg_items') }}
)

select 
    coalesce(i.item_key, c.item_key) as item_key,
    coalesce(i.event_date, c.event_date) as event_date,
    it.seller_id,
    it.category,
    it.title,
    it.price,

    coalesce(impressions, 0) as impressions,
    coalesce(unique_viewers, 0) as unique_viewers,
    coalesce(clicks, 0) as clicks,
    coalesce(unique_clickers, 0) as unique_clickers,

    safe_divide(coalesce(c.clicks, 0), coalesce(i.impressions, 0)) as click_through_rate,

    current_timestamp() as dbt_updated_at

from impressions i
full outer join clicks c 
    on i.item_key = c.item_key 
    and i.event_date = c.event_date
left join items it
    on coalesce(i.item_key, c.item_key) = it.item_key