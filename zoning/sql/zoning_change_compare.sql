-- Zoning change MVP1 vs MVP2 (#33): joins the two outputs on clip; neither version reads the other
-- Run after zoning_change_mvp1.sql and zoning_change_mvp2.sql (same fips_list and PREV/CURR snapshots)
-- Output: one row per CLIP in either version, with both change classes, both outcome groups and MVP2's growth confidence score
-- Dev tables only, so it runs without prd access; zoning_change_compare_parcels.sql adds parcel polygons for CARTO
DECLARE n_examples INT64 DEFAULT 3;  -- C4: example CLIPs and top transitions per move

-- change class -> outcome group (docs/glossary.md "Change classes and outcome groups"); NO_COORDS and R6 stay ungrouped
CREATE TEMP FUNCTION outcome_group(cls STRING) AS (
  CASE
    WHEN cls = 'SLIVER_DROP'                                                                      THEN 'NOISE'
    WHEN cls IN ('CODE_REGIME_SWITCH', 'RENAME_OR_MERGE', 'CITY_PROGRAM', 'MASS_CHANGE')          THEN 'GOVERNMENT_CHANGE'
    WHEN cls IN ('NEEDLE_PARCEL', 'SMALL_TRACT', 'DEVELOPER_TRACT')                               THEN 'DEVELOPMENT_SIGNAL'
  END
);
-- MVP1's SMALL_TRACT is MVP2's DEVELOPER_TRACT (renamed); is_class_changed compares classes after this mapping
CREATE TEMP FUNCTION class_key(cls STRING) AS (
  IF(cls = 'SMALL_TRACT', 'DEVELOPER_TRACT', cls)
);

-- C1: key check. Expect n_rows = n_distinct_clips in both versions, n_only_mvp1 = n_only_mvp2 = 0,
-- and no fips, parcel_shape_id or stage1 (R1-R10) differences
WITH
v1 AS (SELECT clip, fips, county, parcel_shape_id, zoning_change_class FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp1`),
v2 AS (SELECT clip, fips, county, parcel_shape_id, zoning_change_class FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2`),
n AS (
  SELECT county, COUNTIF(v = 1) AS n_rows_mvp1, COUNT(DISTINCT IF(v = 1, clip, NULL)) AS n_distinct_mvp1,
                 COUNTIF(v = 2) AS n_rows_mvp2, COUNT(DISTINCT IF(v = 2, clip, NULL)) AS n_distinct_mvp2
  FROM (SELECT 1 AS v, clip, county FROM v1 UNION ALL SELECT 2, clip, county FROM v2)
  GROUP BY 1
),
j AS (
  SELECT
    COALESCE(v2.county, v1.county) AS county,
    COUNTIF(v1.clip IS NOT NULL AND v2.clip IS NOT NULL) AS n_both,
    COUNTIF(v2.clip IS NULL)                             AS n_only_mvp1,
    COUNTIF(v1.clip IS NULL)                             AS n_only_mvp2,
    COUNTIF(v1.fips != v2.fips)                          AS n_fips_diff,
    COUNTIF(v1.clip IS NOT NULL AND v2.clip IS NOT NULL
      AND v1.parcel_shape_id IS DISTINCT FROM v2.parcel_shape_id) AS n_parcel_mismatch,
    COUNTIF(
      IF(v1.zoning_change_class IN ('CODE_REGIME_SWITCH', 'RENAME_OR_MERGE', 'SLIVER_DROP'), v1.zoning_change_class, 'CANDIDATE')
      != IF(v2.zoning_change_class IN ('CODE_REGIME_SWITCH', 'RENAME_OR_MERGE', 'SLIVER_DROP'), v2.zoning_change_class, 'CANDIDATE')
    )                                                    AS n_stage1_diff
  FROM v1
  FULL JOIN v2 USING (clip)
  GROUP BY 1
)
SELECT * FROM n JOIN j USING (county)
ORDER BY county;

-- Comparison table: FULL OUTER JOIN on clip; shared attributes prefer MVP2 (identical when n_parcel_mismatch = 0)
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_compare` AS
SELECT
  clip,
  COALESCE(v2.fips, v1.fips)                         AS fips,
  COALESCE(v2.county, v1.county)                     AS county,
  v1.clip IS NOT NULL                                AS in_mvp1,
  v2.clip IS NOT NULL                                AS in_mvp2,
  COALESCE(v2.parcel_shape_id, v1.parcel_shape_id)   AS parcel_shape_id,
  v1.clip IS NOT NULL AND v2.clip IS NOT NULL
    AND v1.parcel_shape_id IS DISTINCT FROM v2.parcel_shape_id AS is_parcel_mismatch,
  COALESCE(v2.prev_reg, v1.prev_reg)                 AS prev_reg,
  COALESCE(v2.curr_reg, v1.curr_reg)                 AS curr_reg,
  COALESCE(v2.prev_dset, v1.prev_dset)               AS prev_dset,
  COALESCE(v2.curr_dset, v1.curr_dset)               AS curr_dset,
  COALESCE(v2.transition, v1.transition)             AS transition,
  COALESCE(v2.prev_desc, v1.prev_desc)               AS prev_desc,
  COALESCE(v2.curr_desc, v1.curr_desc)               AS curr_desc,
  COALESCE(v2.transition_desc, v1.transition_desc)   AS transition_desc,
  -- change class and outcome group per version
  v1.zoning_change_class                             AS zoning_change_class_mvp1,
  v2.zoning_change_class                             AS zoning_change_class_mvp2,
  outcome_group(v1.zoning_change_class)              AS outcome_group_mvp1,
  outcome_group(v2.zoning_change_class)              AS outcome_group_mvp2,
  -- TRUE also when the CLIP is in only one version
  class_key(v1.zoning_change_class) IS DISTINCT FROM class_key(v2.zoning_change_class)                 AS is_class_changed,
  outcome_group(v1.zoning_change_class) IS DISTINCT FROM outcome_group(v2.zoning_change_class)         AS is_group_changed,
  -- MVP2 growth confidence score and its inputs (NULL for non-candidates)
  v2.growth_score,
  v2.growth_confidence,
  v2.growth_score_inputs,
  v2.change_pattern,
  v2.has_owner_evidence,
  v2.has_land_evidence,
  v2.yb_status,
  v2.patch_id,
  v2.patch_n,
  -- MVP1 spatial features (R12, R13)
  v1.k_same_tr_share,
  v1.n_same_tr_radius,
  -- shared CLIP attributes
  COALESCE(v2.is_partial_rezone, v1.is_partial_rezone) AS is_partial_rezone,
  COALESCE(v2.is_reg_move, v1.is_reg_move)           AS is_reg_move,
  COALESCE(v2.is_growth_clip, v1.is_growth_clip)     AS is_growth_clip,
  COALESCE(v2.owner_name, v1.owner_name)             AS owner_name,
  COALESCE(v2.max_yr_built, v1.max_yr_built)         AS max_yr_built,
  COALESCE(v2.is_super_parcel, v1.is_super_parcel)   AS is_super_parcel,
  COALESCE(v2.sp_id, v1.sp_id)                       AS sp_id,
  COALESCE(v2.sp_owner_name, v1.sp_owner_name)       AS sp_owner_name,
  COALESCE(v2.lat, v1.lat)                           AS lat,
  COALESCE(v2.lon, v1.lon)                           AS lon,
  COALESCE(v2.geom, v1.geom)                         AS geom
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp1` AS v1
FULL JOIN `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2` AS v2 USING (clip);

-- C2: MVP1 change class x MVP2 change class x growth confidence, per county
SELECT
  county,
  zoning_change_class_mvp1,
  zoning_change_class_mvp2,
  is_class_changed,
  COUNT(*)                              AS n_clips,
  COUNTIF(growth_confidence = 'HIGH')   AS n_high,
  COUNTIF(growth_confidence = 'MEDIUM') AS n_medium,
  COUNTIF(growth_confidence = 'LOW')    AS n_low,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY county), 1) AS pct_of_county
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_compare`
GROUP BY 1, 2, 3, 4
ORDER BY county, zoning_change_class_mvp1, n_clips DESC;

-- C3: MVP1 outcome group x MVP2 outcome group x growth confidence, per county
SELECT
  county,
  outcome_group_mvp1,
  outcome_group_mvp2,
  is_group_changed,
  COUNT(*)                              AS n_clips,
  COUNTIF(growth_confidence = 'HIGH')   AS n_high,
  COUNTIF(growth_confidence = 'MEDIUM') AS n_medium,
  COUNTIF(growth_confidence = 'LOW')    AS n_low,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY county), 1) AS pct_of_county
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_compare`
GROUP BY 1, 2, 3, 4
ORDER BY county, outcome_group_mvp1, n_clips DESC;

-- C4: biggest moves (changed class pairs by CLIP count), with top transitions and example CLIPs (highest growth score first)
SELECT
  county,
  zoning_change_class_mvp1,
  zoning_change_class_mvp2,
  is_group_changed,
  COUNT(*)                                  AS n_clips,
  COUNT(DISTINCT patch_id)                  AS n_patches,
  ROUND(AVG(growth_score), 2)               AS avg_growth_score,
  APPROX_TOP_COUNT(transition, n_examples)  AS top_transitions,
  ARRAY_AGG(STRUCT(clip, transition, growth_score, growth_score_inputs, owner_name)
    ORDER BY growth_score DESC, clip LIMIT 3) AS examples  -- LIMIT must be a literal: keep = n_examples
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_compare`
WHERE is_class_changed
GROUP BY 1, 2, 3, 4
ORDER BY county, n_clips DESC;
