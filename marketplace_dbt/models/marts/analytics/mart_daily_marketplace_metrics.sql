{{
    config(
        materialized='incremental',
        unique_key='metric_date',
        partition_by={
            'field': 'metric_date', 
            'data_type': 'date'
        }
    )
}}

with daily_purchases as (
    select
        purchase_date as metric_date,
        count(distinct purchase_key) as total_purchases,
        count(distinct buyer_id) as unique_buyers,
        count(distinct item_key) as unique_items_sold,
        sum(purchase_amount) as gross_merchandise_value,
        avg(purchase_amount) as average_order_value,
        sum(platform_fee) as platform_revenue
    from {{ ref('stg_purchases') }}
    {% if is_incremental() %}
    where purchase_date > (select max(metric_date) from {{ this }})
    {% endif %}
    group by metric_date
),

daily_listings as (
    select 
        listed_date as metric_date,
        count(*) as new_listings,
        count(distinct seller_id) as active_sellers
    from {{ ref('stg_items') }}
    {% if is_incremental() %}
    where listed_date > (select max(metric_date) from {{ this }})
    {% endif %}
    group by metric_date
),

daily_users as (
    select 
        signup_date as metric_date,
        count(*) as new_users
    from {{ ref('stg_users') }}
    {% if is_incremental() %}
    where signup_date > (select max(metric_date) from {{ this }})
    {% endif %}
    group by metric_date
),

daily_engagement as (
    select 
        impression_date as metric_date,
        count(distinct user_id) as daily_active_users,
        count(*) as total_impressions
    from {{ ref('stg_impressions') }}
    {% if is_incremental() %}
    where impression_date > (select max(metric_date) from {{ this }})
    {% endif %}
    group by metric_date
),

daily_searches as (
    select
        click_date as metric_date,
        count(*) as total_searches,
        count(distinct user_id) as users_searching
    from {{ ref('stg_clicks') }}
    {% if is_incremental() %}
    where click_date > (select max(metric_date) from {{ this }})
    {% endif %}
    group by metric_date
)

select
    coalesce(p.metric_date, l.metric_date, u.metric_date, e.metric_date, s.metric_date) as metric_date,
    
    -- Revenue metrics
    coalesce(p.total_purchases, 0) as purchases,
    coalesce(p.gross_merchandise_value, 0) as gross_merchandise_value,
    coalesce(p.platform_revenue, 0) as platform_revenue,
    coalesce(p.average_order_value, 0) as avg_order_value,
    
    -- Marketplace health
    coalesce(l.new_listings, 0) as new_listings,
    coalesce(p.unique_items_sold, 0) as items_sold,
    safe_divide(p.unique_items_sold, l.new_listings) as sell_through_rate,
    
    -- User metrics
    coalesce(u.new_users, 0) as new_users,
    coalesce(e.daily_active_users, 0) as daily_active_users,
    coalesce(p.unique_buyers, 0) as buyers,
    coalesce(l.active_sellers, 0) as sellers,
    
    -- Engagement metrics
    coalesce(e.total_impressions, 0) as impressions,
    coalesce(s.total_searches, 0) as searches,
    coalesce(s.users_searching, 0) as users_searching,
    
    current_timestamp() as dbt_updated_at

from daily_purchases p
full outer join daily_listings l on p.metric_date = l.metric_date
full outer join daily_users u on coalesce(p.metric_date, l.metric_date) = u.metric_date
full outer join daily_engagement e on coalesce(p.metric_date, l.metric_date, u.metric_date) = e.metric_date
full outer join daily_searches s on coalesce(p.metric_date, l.metric_date, u.metric_date, e.metric_date) = s.metric_date

where coalesce(p.metric_date, l.metric_date, u.metric_date, e.metric_date, s.metric_date) is not null