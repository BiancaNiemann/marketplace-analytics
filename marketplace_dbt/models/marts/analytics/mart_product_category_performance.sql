{{
  config(
    materialized='table'
  )
}}

with category_listings as (
    select
        category,
        title as sub_category,
        count(*) as total_listings,
        count(case when status = 'active' then 1 end) as active_listings,
        count(case when status = 'sold' then 1 end) as sold_listings,
        count(case when status = 'removed' then 1 end) as removed_listings,
        avg(price) as avg_price
    from {{ ref('stg_items') }}
    group by category, title
),

category_sales as (
    select
        i.category,
        i.title as sub_category,
        count(distinct p.purchase_id) as total_sales,
        sum(p.purchase_amount) as total_revenue,
        avg(p.purchase_amount) as avg_sale_price,
        avg(DATE_DIFF(p.purchase_date, i.listed_date, day)) as avg_days_to_sell
    from {{ ref('stg_purchases') }} p
    join {{ ref('stg_items') }} i on p.item_key = i.item_key
    group by category, sub_category
),

category_engagement as (
    select
        i.category,
        i.title as sub_category,
        sum(ie.impressions) as total_impressions,
        sum(ie.clicks) as total_clicks,
        avg(ie.click_through_rate) as avg_ctr
    from {{ ref('fct_item_engagement') }} ie
    join {{ ref('stg_items') }} i on ie.item_key = i.item_key
    group by category, sub_category
)

select
    cl.category,
    cl.sub_category,
    
    -- Listing metrics
    cl.total_listings,
    cl.active_listings,
    cl.sold_listings,
    cl.removed_listings,
    safe_divide(cl.sold_listings, cl.total_listings) as sell_through_rate,
    
    -- Pricing
    cl.avg_price,
    coalesce(cs.avg_sale_price, 0) as avg_sale_price,
    
    -- Sales metrics
    coalesce(cs.total_sales, 0) as total_sales,
    coalesce(cs.total_revenue, 0) as total_revenue,
    coalesce(cs.avg_days_to_sell, 0) as avg_days_to_sell,
    
    -- Engagement
    coalesce(ce.total_impressions, 0) as total_impressions,
    coalesce(ce.total_clicks, 0) as total_clicks,
    coalesce(ce.avg_ctr, 0) as avg_click_through_rate,
    
    -- Conversion metrics
    safe_divide(cs.total_sales, ce.total_clicks) as click_to_purchase_rate,
    
    current_timestamp() as dbt_updated_at

from category_listings cl
left join category_sales cs on cl.category = cs.category and cl.sub_category = cs.sub_category
left join category_engagement ce on cl.category = ce.category and cl.sub_category = ce.sub_category