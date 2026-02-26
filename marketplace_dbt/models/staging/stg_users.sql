select 
    cast(user_id as int64) as user_id,
    cast(signup_ts as timestamp) as signup_at,
    country,
    city,
    age_group,
    cast(is_seller as boolean) as is_seller,
    account_type,
    signup_channel,
    device_type,
    cast(marketing_opt_in as boolean) as marketing_opt_in,
    cast(is_verified as boolean) as is_verified,
    status
from {{ source('raw', 'users') }}