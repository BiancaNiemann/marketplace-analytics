with items_base as (
    select * from {{ ref('stg_items') }}
),

item_engagement as (
    select
        item_key,
        count(distinct user_key) as unique_viewers,
        count(*) as total_views
    from {{ ref('stg_impressions') }}
    group by item_key
),

item_clicks as (
    select
        item_key,
        count(distinct user_key) as unique_clickers,
        count(*) as total_clicks
    from {{ ref('stg_clicks') }}
    group by item_key
)

select
    i.item_key,
    i.seller_id,
    i.title,
    i.category,
    i.condition,
    i.price,
    i.listed_date,
    i.status,
    i.sold_date,

    -- Engagement Metrics
    coalesce(ie.unique_viewers, 0) as unique_viewers,
    coalesce(ie.total_views, 0) as total_views,
    coalesce(ic.unique_clickers, 0) as unique_clickers,
    coalesce(ic.total_clicks, 0) as total_clicks,

    -- Calculated Metrics
    safe_divide(ic.total_clicks, ie.total_views) as click_through_rate,
    case when i.sold_date is not null 
        then date_diff(date(i.sold_date), date(i.listed_date), day) 
        else null
    end as days_to_sell,

    current_timestamp() as dbt_updated_at

from items_base i
left join item_engagement ie on cast(i.item_key as string) = cast(ie.item_key as string)
left join item_clicks ic on cast(i.item_key as string) = cast(ic.item_key as string)
