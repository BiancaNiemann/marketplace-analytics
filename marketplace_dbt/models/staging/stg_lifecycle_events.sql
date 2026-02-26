select  
    cast(event_id as int64) as event_id,
    cast(user_id as int64) as user_id,
    event_type,
    cast(timestamp as timestamp) as event_at
from {{ source('raw', 'lifecycle_events') }}