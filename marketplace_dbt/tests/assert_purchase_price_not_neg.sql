SELECT *
FROM {{ ref('stg_purchases') }}
WHERE price < 0