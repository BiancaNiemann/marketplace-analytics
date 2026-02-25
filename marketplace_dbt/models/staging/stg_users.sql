SELECT 
    CAST(user_id AS INT64) AS user_id,
    CAST(signup_ts AS TIMESTAMP) AS signup_at,
    country,
    city,
    age_group,
    CAST(is_seller AS BOOLEAN) AS is_seller,
    account_type,
    signup_channel,
    device_type,
    CAST(marketing_opt_in AS BOOLEAN) AS marketing_opt_in,
    CAST(is_verified AS BOOLEAN) AS is_verified,
    status
FROM {{ source('raw', 'users') }}