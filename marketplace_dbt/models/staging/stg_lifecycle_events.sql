SELECT  
    CAST(event_id AS INT64) AS event_id,
    CAST(user_id AS INT64) AS user_id,
    event_type,
    CAST(timestamp AS TIMESTAMP) AS event_at
FROM {{ source('raw', 'lifecycle_events') }}