SELECT
    CAST(search_id AS INT64) AS search_id,
    CAST(user_id AS INT64) AS user_id,
    query AS search_query,
    CAST(timestamp AS TIMESTAMP) AS searched_at
FROM {{ source('raw', 'search_events') }}