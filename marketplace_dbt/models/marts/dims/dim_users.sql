with user_base as (
    select *
    from {{ ref('stg_users') }}
),

user_activity as (
    select
        user_id,
        min(event_at) as first_event_date,
        max(event_at) as last_event_date,
        count(DISTINCT event_at) as days_active
    from {{ ref('stg_lifecycle_events') }}
    group by user_id
),

user_purchases as (
    select
        buyer_id,
        count(*) as total_purchases,
        sum(price) as lifetime_value,
        min(purchased_at) as first_purchase_date,
        max(purchased_at) as last_purchase_date
    from {{ ref('stg_purchases') }}
    group by buyer_id
),

user_listings as (
    select
        seller_id,
        count(*) as total_listings,
        count(DISTINCT case when status = "sold" then item_id end) as items_sold,
        sum(case when status = "sold" then price end) as total_revenue
    from {{ ref('stg_items') }}
    group by seller_id
)

select
    ub.user_id,
    ub.signup_at,
    ub.country,
    ub.city,    
    ub.age_group,
    ub.account_type,
    ub.signup_channel,
    ub.device_type,
    ub.marketing_opt_in,
    ub.is_verified,
    ub.status,

    -- Activity metrics
    coalesce(ua.first_event_date, ub.signup_at) as first_seen_at,
    ua.last_event_date as last_seen_at,
    ua.days_active,

    -- Buyer metrics
    coalesce(up.total_purchases, 0) as total_purchases,
    coalesce(up.lifetime_value, 0) as buyer_lifetime_value,
    up.first_purchase_date,
    up.last_purchase_date,

    -- Seller metrics
    coalesce(ul.total_listings, 0) as total_listings,
    coalesce(ul.items_sold, 0) as items_sold,
    coalesce(ul.total_revenue, 0) as seller_total_revenue,

    -- Segmentation
    case 
        when up.total_purchases > 0 and ul.total_listings > 0 then 'Buyer & Seller'
        when up.total_purchases > 0 then 'Buyer Only'
        when ul.total_listings > 0 then 'Seller Only'
        else 'Inactive'
    end as user_segment,

    case
        when date_diff(current_date(), coalesce(date(ua.last_event_date), date(ub.signup_at)), day) <= 30 then 'Active'
        when date_diff(current_date(), coalesce(date(ua.last_event_date), date(ub.signup_at)), day) <= 90 then 'At Risk'
        else 'Churned'
    end as activity_status,

    current_timestamp() as dbt_updated_at

from user_base ub
left join user_activity ua ON ub.user_id = ua.user_id
left join user_purchases up ON ub.user_id = up.buyer_id
left join user_listings ul ON ub.user_id = ul.seller_id