CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.work_eprashar.growth_intelligence_base_qgis_eg_og_nclip` AS

-- CTE 1: Calculate the V2 indicator for New Construction Permits
-- In terms of stages, this impacts ONGOING GROWTH 
WITH NewPermitV2 AS (
  WITH NewPermitV1 AS (
    SELECT
      puid,
      addr_vacant_ratio,
      EXTRACT(YEAR FROM current_date) AS yr_now,
      COALESCE(latest_year_built, latest_eff_year_built) AS max_yr_built
    FROM `clgx-gis-app-dev-06e3.work_eprashar.growth_intelligence_base_qgis`
    WHERE 
      recent_new_con_bldg_permit_indicator = 'Y'
  )
  SELECT
    puid,
    CASE 
      WHEN max_yr_built IS NULL
        OR addr_vacant_ratio > 0
        --OR (yr_now - max_yr_built <= 2) Final decision was to exclude this condition
      THEN 'Y'
      ELSE 'N' 
    END AS recent_new_con_bldg_permit_indicator_V2
  FROM NewPermitV1
),
-- CTE 2: Calculate the V2 indicator for Builder/Developer Ownership
-- In terms of stages, this impacts EARLY GROWTH
BuilDevelopV2 AS (
  WITH LatestTransaction AS (
    SELECT
      puid,
      MAX(recordingdt) AS max_transaction_date
    FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_transactions.transaction_v1`
    GROUP BY puid
  ),
  BuilDevelopV1 AS (
    SELECT
      a.puid,
      COALESCE(a.latest_year_built, a.latest_eff_year_built) AS max_yr_built,
      a.addr_vacant_ratio,
      b.max_transaction_date
    FROM `clgx-gis-app-dev-06e3.work_eprashar.growth_intelligence_base_qgis` AS a
    LEFT JOIN LatestTransaction AS b ON a.puid = b.puid
    WHERE a.builder_developer_ownership_indicator = 'Y'
  )
  SELECT
    puid,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), SAFE.PARSE_DATE('%Y%m%d', CAST(max_transaction_date AS STRING)), DAY) > (5*365) THEN 'N'
      WHEN (EXTRACT(YEAR FROM CURRENT_DATE()) - max_yr_built > 2 AND (addr_vacant_ratio = 0 OR addr_vacant_ratio IS NULL)) THEN 'N'
      ELSE 'Y'
    END AS builder_developer_ownership_indicator_V2
  FROM BuilDevelopV1
),
-- CTE 3: Calculate the V2 indicator for Land Use Change
-- In terms of stages, this impacts EARLY GROWTH
LandUseV2 AS (
  WITH LandUseV1 AS (
    SELECT
      puid,
      addr_vacant_ratio,
      EXTRACT(YEAR FROM current_date) AS yr_now,
      COALESCE(latest_year_built, latest_eff_year_built) AS max_yr_built
    FROM `clgx-gis-app-dev-06e3.work_eprashar.growth_intelligence_base_qgis`
    WHERE 
      landuse_change_indicator = 'Y'
  )
  SELECT
    puid,
    CASE 
      WHEN max_yr_built IS NULL
        OR addr_vacant_ratio > 0
        -- OR (yr_now - max_yr_built <= 2) Excluded this condition
      THEN 'Y'
      ELSE 'N' 
    END AS land_use_change_indicator_V2
  FROM LandUseV1
),
-- CTE 4: Calculate V2 indicator for New Clip
-- In terms of stages, this didn't impact anything before but will impact EARLY GROWTH now
NewClipV2 AS (
  WITH NewClipV1 AS (
    SELECT
      puid,
      addr_vacant_ratio,
      COALESCE(latest_year_built, latest_eff_year_built) AS max_yr_built,
      FROM `clgx-gis-app-dev-06e3.work_eprashar.growth_intelligence_base_qgis`
      WHERE new_clip_indicator = 'Y'
  )
  SELECT
    puid,
    CASE WHEN NewClipV1.max_yr_built IS NULL
    OR addr_vacant_ratio > 0
    THEN 'Y' ELSE 'N'
    END AS new_clip_indicator_V2
  FROM NewClipV1 
)
-- The final SELECT statement reconstructs the table, adding all three new V2 columns
SELECT
  -- Select all columns from the original base table
  base.*,
  
  -- New V2 indicator for permits
  v_permit.recent_new_con_bldg_permit_indicator_V2,

  -- New V2 indicator for the builder/developer flag
  v_build.builder_developer_ownership_indicator_V2,

  -- New V2 indicator for the land use change flag
  v_land.land_use_change_indicator_V2,

  -- New V2 indicator for new clip flag
  v_clip.new_clip_indicator_V2

FROM 
  `clgx-gis-app-dev-06e3.work_eprashar.growth_intelligence_base_qgis` AS base
LEFT JOIN 
  NewPermitV2 AS v_permit ON base.puid = v_permit.puid
LEFT JOIN 
  BuilDevelopV2 AS v_build ON base.puid = v_build.puid
LEFT JOIN
  LandUseV2 AS v_land ON base.puid = v_land.puid
LEFT JOIN NewClipV2 AS v_clip ON base.puid = v_clip.puid;