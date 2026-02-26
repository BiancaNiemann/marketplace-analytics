select
    cast(impression_id as int64) as impression_id,
    cast(search_id as int64) as search_id,
    cast(item_id as int64) as item_id,
    cast(position as int64) as position,
    cast(timestamp as timestamp) as impression_at
from {{ source('raw', 'impressions') }}
