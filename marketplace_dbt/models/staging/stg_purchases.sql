select
    cast(purchase_id as int64) as purchase_id,
    cast(buyer_id as int64) as buyer_id,
    cast(item_id as int64) as item_id,
    cast(price as numeric) as price,
    cast(purchase_ts as timestamp) as purchased_at
from {{ source('raw', 'purchases') }}