SELECT
    CAST(notification_id AS INT64) AS notification_id,
    CAST(user_id AS INT64) AS user_id,
    type AS notification_type,
    CAST(sent_ts AS TIMESTAMP) AS sent_at,
    CAST(opened AS BOOLEAN) AS notification_opened
FROM {{ source('raw', 'notifications') }}