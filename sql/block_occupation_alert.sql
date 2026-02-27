-- Required output columns:
-- block_key, car_id, block_type, start_at_utc, end_at_utc, occupied_days
--
-- IMPORTANT:
-- 1) This default query reads socar_occupation.car_occupation.
-- 2) If your production source has explicit block_type, replace `typed` CTE accordingly.
-- 3) Keep query parameters: @min_days (INT64), @target_block_types (ARRAY<STRING>).

WITH base AS (
  SELECT
    id,
    car_id,
    start_at,
    end_at,
    state,
    occupant_id
  FROM `socar-data.socar_occupation.car_occupation`
  WHERE occupant_type = 'BLOCK'
    AND state = 'CONFIRMED'
),
typed AS (
  SELECT
    id,
    car_id,
    start_at,
    end_at,
    state,
    occupant_id,
    SPLIT(occupant_id, '_')[SAFE_OFFSET(1)] AS raw_block_code,
    CASE
      -- TODO: map your real source code to canonical type names below.
      -- Example:
      -- WHEN SPLIT(occupant_id, '_')[SAFE_OFFSET(1)] = 'OUTAGE' THEN 'OUTAGE'
      -- WHEN SPLIT(occupant_id, '_')[SAFE_OFFSET(1)] = 'CUSTOMER_SERVICE' THEN 'CUSTOMER_SERVICE'
      -- WHEN SPLIT(occupant_id, '_')[SAFE_OFFSET(1)] = 'MAINTENANCE' THEN 'MAINTENANCE'
      -- WHEN SPLIT(occupant_id, '_')[SAFE_OFFSET(1)] = 'CAR_ACCIDENT' THEN 'CAR_ACCIDENT'
      ELSE SPLIT(occupant_id, '_')[SAFE_OFFSET(1)]
    END AS block_type
  FROM base
),
filtered AS (
  SELECT
    CONCAT(CAST(car_id AS STRING), '|', CAST(start_at AS STRING), '|', IFNULL(block_type, 'UNKNOWN')) AS block_key,
    car_id,
    IFNULL(block_type, 'UNKNOWN') AS block_type,
    start_at AS start_at_utc,
    end_at AS end_at_utc,
    TIMESTAMP_DIFF(COALESCE(end_at, CURRENT_TIMESTAMP()), start_at, DAY) AS occupied_days
  FROM typed
  WHERE TIMESTAMP_DIFF(COALESCE(end_at, CURRENT_TIMESTAMP()), start_at, DAY) >= @min_days
    AND (
      IFNULL(block_type, 'UNKNOWN') IN UNNEST(@target_block_types)
      OR raw_block_code IN UNNEST(@target_block_types)
    )
)
SELECT *
FROM filtered
ORDER BY occupied_days DESC, start_at_utc ASC;
