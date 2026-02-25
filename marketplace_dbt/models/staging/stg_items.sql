SELECT
    CAST(item_id AS INT64) AS item_id,
    CAST(seller_id AS INT64) AS seller_id,
    title AS item_title,
    category,
    CAST(price AS NUMERIC) AS price,
    CAST(created_ts AS TIMESTAMP) AS created_at,
    status
FROM {{ source('raw', 'items') }}