SELECT *
FROM {{ ref('stg_purchases') }}
WHERE purchase_amount < 0