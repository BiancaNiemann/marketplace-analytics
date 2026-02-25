SELECT
    CAST(impression_id AS INT64) AS impression_id,
    CAST(search_id AS INT64) AS search_id,
    CAST(item_id AS INT64) AS item_id,
    CAST(position AS INT64) AS position,
    CAST(timestamp AS TIMESTAMP) AS impression_at
FROM {{ source('raw', 'impressions') }}
