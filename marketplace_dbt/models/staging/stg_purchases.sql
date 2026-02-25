SELECT
    CAST(purchase_id AS INT64) AS purchase_id,
    CAST(buyer_id AS INT64) AS buyer_id,
    CAST(item_id AS INT64) AS item_id,
    CAST(price AS NUMERIC) AS price,
    CAST(purchase_ts AS TIMESTAMP) AS purchased_at
FROM {{ source('raw', 'purchases') }}