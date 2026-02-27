-- Load Historical Data from GCS to BigQuery
-- This query was run directly in BQ to get all the old data from the bucket into theraw tables folder.

-- ============================================================================
-- STEP 1: Load USERS data from all months
-- ============================================================================

-- Create or replace the users table by loading all months at once
LOAD DATA OVERWRITE `marketplace-analytics-485915.raw.users`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = [
    'gs://marketplace-analytics-485915-data-lake/raw/2025-01/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-02/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-03/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-04/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-05/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-06/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-07/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-08/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-09/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-10/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-11/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-12/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-01/users.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-02/users.csv'
  ]
);

-- Verify users loaded
SELECT 
  'users' as table_name,
  COUNT(*) as row_count,
  MIN(signup_date) as earliest_date,
  MAX(signup_date) as latest_date
FROM `marketplace-analytics-485915.raw.users`;

-- ============================================================================
-- STEP 2: Load ITEMS data from all months
-- ============================================================================

LOAD DATA OVERWRITE `marketplace-analytics-485915.raw.items`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = [
    'gs://marketplace-analytics-485915-data-lake/raw/2025-01/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-02/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-03/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-04/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-05/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-06/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-07/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-08/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-09/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-10/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-11/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-12/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-01/items.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-02/items.csv'
  ]
);

SELECT COUNT(*) as items_count FROM `marketplace-analytics-485915.raw.items`;

-- ============================================================================
-- STEP 3: Load PURCHASES data from all months
-- ============================================================================

LOAD DATA OVERWRITE `marketplace-analytics-485915.raw.purchases`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = [
    'gs://marketplace-analytics-485915-data-lake/raw/2025-01/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-02/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-03/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-04/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-05/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-06/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-07/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-08/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-09/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-10/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-11/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-12/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-01/purchases.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-02/purchases.csv'
  ]
);

SELECT COUNT(*) as purchases_count FROM `marketplace-analytics-485915.raw.purchases`;

-- ============================================================================
-- STEP 4: Load CLICKS data from all months
-- ============================================================================

LOAD DATA OVERWRITE `marketplace-analytics-485915.raw.clicks`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = [
    'gs://marketplace-analytics-485915-data-lake/raw/2025-01/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-02/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-03/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-04/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-05/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-06/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-07/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-08/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-09/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-10/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-11/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-12/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-01/clicks.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-02/clicks.csv'
  ]
);

SELECT COUNT(*) as clicks_count FROM `marketplace-analytics-485915.raw.clicks`;

-- ============================================================================
-- STEP 5: Load IMPRESSIONS data from all months
-- ============================================================================

LOAD DATA OVERWRITE `marketplace-analytics-485915.raw.impressions`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = [
    'gs://marketplace-analytics-485915-data-lake/raw/2025-01/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-02/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-03/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-04/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-05/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-06/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-07/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-08/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-09/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-10/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-11/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-12/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-01/impressions.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-02/impressions.csv'
  ]
);

SELECT COUNT(*) as impressions_count FROM `marketplace-analytics-485915.raw.impressions`;

-- ============================================================================
-- STEP 6: Load SEARCH_EVENTS data from all months
-- ============================================================================

LOAD DATA OVERWRITE `marketplace-analytics-485915.raw.search_events`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = [
    'gs://marketplace-analytics-485915-data-lake/raw/2025-01/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-02/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-03/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-04/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-05/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-06/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-07/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-08/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-09/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-10/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-11/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-12/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-01/search_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-02/search_events.csv'
  ]
);

SELECT COUNT(*) as search_events_count FROM `marketplace-analytics-485915.raw.search_events`;

-- ============================================================================
-- STEP 7: Load NOTIFICATIONS data from all months
-- ============================================================================

LOAD DATA OVERWRITE `marketplace-analytics-485915.raw.notifications`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = [
    'gs://marketplace-analytics-485915-data-lake/raw/2025-01/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-02/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-03/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-04/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-05/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-06/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-07/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-08/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-09/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-10/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-11/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-12/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-01/notifications.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-02/notifications.csv'
  ]
);

SELECT COUNT(*) as notifications_count FROM `marketplace-analytics-485915.raw.notifications`;

-- ============================================================================
-- STEP 8: Load LIFECYCLE_EVENTS data from all months
-- ============================================================================

LOAD DATA OVERWRITE `marketplace-analytics-485915.raw.lifecycle_events`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = [
    'gs://marketplace-analytics-485915-data-lake/raw/2025-01/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-02/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-03/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-04/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-05/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-06/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-07/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-08/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-09/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-10/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-11/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2025-12/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-01/lifecycle_events.csv',
    'gs://marketplace-analytics-485915-data-lake/raw/2026-02/lifecycle_events.csv'
  ]
);

SELECT COUNT(*) as lifecycle_events_count FROM `marketplace-analytics-485915.raw.lifecycle_events`;

-- ============================================================================
-- STEP 9: Verify all data loaded correctly
-- ============================================================================

-- Summary of all tables
SELECT 'users' as table_name, COUNT(*) as row_count FROM `marketplace-analytics-485915.raw.users`
UNION ALL
SELECT 'items', COUNT(*) FROM `marketplace-analytics-485915.raw.items`
UNION ALL
SELECT 'purchases', COUNT(*) FROM `marketplace-analytics-485915.raw.purchases`
UNION ALL
SELECT 'clicks', COUNT(*) FROM `marketplace-analytics-485915.raw.clicks`
UNION ALL
SELECT 'impressions', COUNT(*) FROM `marketplace-analytics-485915.raw.impressions`
UNION ALL
SELECT 'search_events', COUNT(*) FROM `marketplace-analytics-485915.raw.search_events`
UNION ALL
SELECT 'notifications', COUNT(*) FROM `marketplace-analytics-485915.raw.notifications`
UNION ALL
SELECT 'lifecycle_events', COUNT(*) FROM `marketplace-analytics-485915.raw.lifecycle_events`;

-- ============================================================================
-- ALL DONE! 
-- ============================================================================
