-- Create (or replace) the cleaned fact table for the full dataset
CREATE OR REPLACE TABLE `de-zoomcamp-tmall.tmall_data_all.fact_user_behavior` AS
WITH base AS (
  -- 1. Get the raw "messy" column
  SELECT 
    `1770511_u3427707_click_2013-06-28 10:12:51` AS line
  FROM `de-zoomcamp-tmall.tmall_data_all.raw_full_log`
),
regex_extract AS (
  -- 2. Extract fields using Regex patterns: [User ID] [Item ID] [Action] [Timestamp]
  SELECT
    REGEXP_EXTRACT(line, r'^([0-9]+)') AS user_id,
    REGEXP_EXTRACT(line, r'(u[0-9]+)') AS item_id,
    REGEXP_EXTRACT(line, r'(click|cart|alipay|collect)') AS action,
    REGEXP_EXTRACT(line, r'([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})') AS vtime_raw
  FROM base
)
SELECT
    user_id,
    item_id,
    -- Mapping English actions to standard business labels
    CASE 
        WHEN action = 'click' THEN 'Page View'
        WHEN action = 'cart' THEN 'Add to Cart'
        WHEN action = 'alipay' THEN 'Purchase'
        WHEN action = 'collect' THEN 'Favorite'
        ELSE 'Other'
    END AS behavior_type,
    
    -- Transform timestamps into standard BigQuery formats
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', vtime_raw) AS vtime,
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', vtime_raw)) AS action_date,
    EXTRACT(HOUR FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', vtime_raw)) AS action_hour
FROM 
    regex_extract
-- Filter out records where timestamp extraction failed
WHERE vtime_raw IS NOT NULL;