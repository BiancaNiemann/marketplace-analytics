SELECT 
    CAST(click_id AS INT64) AS click_id,
    CAST(search_id AS INT64) AS search_id,
    CAST(item_id AS INT64) AS item_id,
    CAST(position AS INT64) AS position,
    CAST(click_ts AS TIMESTAMP) AS clicked_at
FROM {{ source('raw', 'clicks') }}