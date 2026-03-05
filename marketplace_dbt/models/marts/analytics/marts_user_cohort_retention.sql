{{
  config(
    materialized='table'
  )
}}

with user_cohorts as (
    select
        user_id,
        date_trunc(signup_date, month) as cohort_month
    from {{ ref('stg_users') }}
),

user_activity as (
    select
        user_id,
        date_trunc(event_date, month) as activity_month
    from {{ ref('stg_lifecycle_events') }}
    group by user_id, activity_month
),

cohort_activity as (
    select
        uc.cohort_month,
        ua.activity_month,
        count(distinct ua.user_id) as active_users,
        date_diff(ua.activity_month, uc.cohort_month, month) as months_since_signup
    from user_cohorts uc
    join user_activity ua on uc.user_id = ua.user_id
    group by uc.cohort_month, ua.activity_month, months_since_signup
),

cohort_sizes as (
    select
        cohort_month,
        count(distinct user_id) as cohort_size
    from user_cohorts
    group by cohort_month
)

select
    ca.cohort_month,
    cs.cohort_size,
    ca.activity_month,
    ca.months_since_signup,
    ca.active_users,
    safe_divide(ca.active_users, cs.cohort_size) as retention_rate,
    
    current_timestamp() as dbt_updated_at

from cohort_activity ca
join cohort_sizes cs on ca.cohort_month = cs.cohort_month