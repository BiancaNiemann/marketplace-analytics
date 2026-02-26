select 
    cast(click_id as int64) as click_id,
    cast(search_id as int64) as search_id,
    cast(item_id as int64) as item_id,
    cast(position as int64) as position,
    cast(click_ts as timestamp) as clicked_at
from {{ source('raw', 'clicks') }}