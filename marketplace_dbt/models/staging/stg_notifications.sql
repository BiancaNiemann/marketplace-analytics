select
    cast(notification_id as int64) as notification_id,
    cast(user_id as int64) as user_id,
    type as notification_type,
    cast(sent_ts as timestamp) as sent_at,
    cast(opened as boolean) as notification_opened
from {{ source('raw', 'notifications') }}