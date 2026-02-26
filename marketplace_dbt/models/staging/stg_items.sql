select
    cast(item_id as int64) as item_id,
    cast(seller_id as int64) as seller_id,
    title as item_title,
    category,
    cast(price as numeric) as price,
    cast(created_ts as timestamp) as created_at,
    status
from {{ source('raw', 'items') }}