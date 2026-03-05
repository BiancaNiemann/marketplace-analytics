{{
  config(
    materialized='table'
  )
}}

with seller_listings as (
    select
        seller_id,
        count(*) as total_listings,
        count(case when status = 'active' then 1 end) as active_listings,
        count(case when status = 'sold' then 1 end) as sold_listings,
        avg(price) as avg_listing_price,
        min(listed_date) as first_listing_date,
        max(listed_date) as last_listing_date
    from {{ ref('stg_items') }}
    group by seller_id
),

seller_sales as (
    select
        i.seller_id,
        count(distinct p.purchase_key) as total_sales,
        sum(p.purchase_amount) as total_revenue,
        sum(p.purchase_amount - p.platform_fee) as net_revenue,
        avg(p.purchase_amount) as avg_sale_price,
        min(p.purchase_timestamp) as first_sale_date,
        max(p.purchase_timestamp) as last_sale_date
    from {{ ref('stg_purchases') }} p
    join {{ ref('stg_items') }} i on p.item_key = i.item_key
    group by seller_id
),

seller_engagement as (
    select
        i.seller_id,
        sum(ie.impressions) as total_impressions,
        sum(ie.clicks) as total_clicks,
        avg(ie.click_through_rate) as avg_ctr
    from {{ ref('fct_item_engagement') }} ie
    join {{ ref('stg_items') }} i on ie.item_key = i.item_key
    group by seller_id
)

select
    sl.seller_id,
    
    -- Listing metrics
    sl.total_listings,
    sl.active_listings,
    sl.sold_listings,
    safe_divide(sl.sold_listings, sl.total_listings) as sell_through_rate,
    sl.avg_listing_price,
    
    -- Sales metrics
    coalesce(ss.total_sales, 0) as total_sales,
    coalesce(ss.total_revenue, 0) as total_revenue,
    coalesce(ss.net_revenue, 0) as net_revenue,
    coalesce(ss.avg_sale_price, 0) as avg_sale_price,
    
    -- Engagement metrics
    coalesce(se.total_impressions, 0) as total_impressions,
    coalesce(se.total_clicks, 0) as total_clicks,
    coalesce(se.avg_ctr, 0) as avg_click_through_rate,
    
    -- Time-based metrics
    sl.first_listing_date,
    sl.last_listing_date,
    ss.first_sale_date,
    ss.last_sale_date,
    date_diff(current_date(), sl.last_listing_date, day) as days_since_last_listing,
    
    -- Seller segments
    case
        when ss.total_sales >= 50 then 'Power Seller'
        when ss.total_sales >= 10 then 'Active Seller'
        when ss.total_sales >= 1 then 'Casual Seller'
        else 'New Seller'
    end as seller_tier,
    
    current_timestamp() as dbt_updated_at

from seller_listings sl
left join seller_sales ss on sl.seller_id = ss.seller_id
left join seller_engagement se on sl.seller_id = se.seller_id