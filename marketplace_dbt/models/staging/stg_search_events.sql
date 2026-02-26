select
    cast(search_id as int64) as search_id,
    cast(user_id as int64) as user_id,
    query as search_query,
    cast(timestamp as timestamp) as searched_at
from {{ source('raw', 'search_events') }}