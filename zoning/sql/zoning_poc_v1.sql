-- Zoning change POC v1: sample counties
-- 12095 Orange FL | 12001 Alachua FL | 04013 Maricopa AZ
DECLARE fips_list ARRAY<STRING> DEFAULT ['12095', '12001', '04013'];

CREATE TEMP FUNCTION norm(s STRING) AS (NULLIF(UPPER(TRIM(s)), ''));
CREATE TEMP FUNCTION str_chg(a STRING, b STRING) AS (COALESCE(norm(a), '') != COALESCE(norm(b), ''));
CREATE TEMP FUNCTION num_chg(a FLOAT64, b FLOAT64) AS (
  (a IS NULL) != (b IS NULL)
  OR (a IS NOT NULL AND b IS NOT NULL AND ABS(a - b) > 1e-6 * GREATEST(1, ABS(a), ABS(b)))
);
CREATE TEMP FUNCTION dir(a FLOAT64, b FLOAT64) AS (
  IF(a IS NULL OR b IS NULL OR NOT num_chg(a, b), 0, IF(b > a, 1, -1))
);

CREATE TEMP TABLE prev_s AS
SELECT *, LPAD(fips_code, 5, '0') AS fips
FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_zoning_hist.gridics_zoning_archive_250903`
WHERE LPAD(fips_code, 5, '0') IN UNNEST(fips_list)
  AND clip IS NOT NULL
  AND COALESCE(clgx_pmd_actionflag, '') != 'D';

CREATE TEMP TABLE curr_s AS
SELECT *, LPAD(fips_code, 5, '0') AS fips
FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_zoning.gridics_zoning`
WHERE LPAD(fips_code, 5, '0') IN UNNEST(fips_list)
  AND clip IS NOT NULL
  AND COALESCE(clgx_pmd_actionflag, '') != 'D';

CREATE TEMP TABLE keys_s AS
SELECT 'PREV' AS snapshot, fips, clip, zid, zoning_district, zoning_regulation_name, last_updated FROM prev_s
UNION ALL
SELECT 'CURR' AS snapshot, fips, clip, zid, zoning_district, zoning_regulation_name, last_updated FROM curr_s;

-- ===== STEP A1: county coverage (check the county exists in BOTH snapshots) =====
SELECT
  fips, snapshot,
  COUNT(*)                                AS n_rows,
  COUNT(DISTINCT clip)                    AS n_clips,
  COUNT(*) - COUNT(DISTINCT clip)         AS n_extra_rows,
  COUNTIF(zid IS NULL)                    AS n_null_zid,
  COUNTIF(norm(zoning_district) IS NULL)  AS n_null_district,
  COUNT(DISTINCT norm(zoning_district))   AS n_districts,
  COUNT(DISTINCT zoning_regulation_name)  AS n_regulations,
  MIN(last_updated)                       AS min_last_updated,
  MAX(last_updated)                       AS max_last_updated
FROM keys_s
GROUP BY fips, snapshot
ORDER BY fips, snapshot DESC;

-- ===== STEP A2: what the multi-row CLIPs are =====
SELECT fips, snapshot, dup_kind, COUNT(*) AS n_clips
FROM (
  SELECT
    fips, snapshot, clip,
    CASE
      WHEN COUNT(DISTINCT norm(zoning_district)) > 1 THEN 'MULTI_DISTRICT'   -- split-zoned
      WHEN COUNT(DISTINCT zid) > 1                   THEN 'SAME_DISTRICT_MULTI_ZID'
      ELSE 'DUPLICATE'
    END AS dup_kind
  FROM keys_s
  GROUP BY fips, snapshot, clip
  HAVING COUNT(*) > 1
)
GROUP BY 1, 2, 3
ORDER BY 1, 2 DESC, 3;

-- ===== STEP B: per-CLIP change table (persisted for investigation) =====
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v1_sample` AS
WITH
prev_sets AS (
  SELECT clip,
    COUNT(*)                                AS n_rows,
    COUNT(DISTINCT norm(zoning_district))   AS n_districts,
    STRING_AGG(DISTINCT norm(zoning_district), ' | ' ORDER BY norm(zoning_district)) AS district_set,
    STRING_AGG(DISTINCT zoning_district, ' | ' ORDER BY zoning_district)             AS raw_district_set
  FROM prev_s GROUP BY clip
),
curr_sets AS (
  SELECT clip,
    COUNT(*)                                AS n_rows,
    COUNT(DISTINCT norm(zoning_district))   AS n_districts,
    STRING_AGG(DISTINCT norm(zoning_district), ' | ' ORDER BY norm(zoning_district)) AS district_set,
    STRING_AGG(DISTINCT zoning_district, ' | ' ORDER BY zoning_district)             AS raw_district_set
  FROM curr_s GROUP BY clip
),
-- One representative row per CLIP, chosen deterministically so unchanged split-zoned CLIPs line up
prev AS (
  SELECT p.*, s.n_rows, s.n_districts, s.district_set, s.raw_district_set
  FROM prev_s p JOIN prev_sets s USING (clip)
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.clip
    ORDER BY norm(p.zoning_district) NULLS LAST, p.zid, p.clgx_pmd_load_timestamp DESC) = 1
),
curr AS (
  SELECT c.*, s.n_rows, s.n_districts, s.district_set, s.raw_district_set
  FROM curr_s c JOIN curr_sets s USING (clip)
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.clip
    ORDER BY norm(c.zoning_district) NULLS LAST, c.zid, c.clgx_pmd_load_timestamp DESC) = 1
),
joined AS (
  SELECT
    COALESCE(c.clip, p.clip)               AS clip,
    COALESCE(c.fips, p.fips)               AS fips,
    COALESCE(c.market_name, p.market_name) AS market_name,
    CASE WHEN p.clip IS NULL THEN 'ADDED' WHEN c.clip IS NULL THEN 'REMOVED' ELSE 'BOTH' END AS presence,

    p.n_rows AS prev_n_rows,               c.n_rows AS curr_n_rows,
    p.n_districts AS prev_n_districts,     c.n_districts AS curr_n_districts,
    p.district_set AS prev_district_set,   c.district_set AS curr_district_set,
    p.zid AS prev_zid,                     c.zid AS curr_zid,
    p.zoning_district AS prev_district,    c.zoning_district AS curr_district,
    p.zone_district_description AS prev_desc,     c.zone_district_description AS curr_desc,
    p.zoning_regulation_name AS prev_regulation,  c.zoning_regulation_name AS curr_regulation,
    p.planned_development_pd AS prev_pd,   c.planned_development_pd AS curr_pd,
    p.lot_area_gis AS prev_lot_area,       c.lot_area_gis AS curr_lot_area,
    p.floor_area_ratio AS prev_far,        c.floor_area_ratio AS curr_far,
    p.maximum_building_height_feet AS prev_height_ft,       c.maximum_building_height_feet AS curr_height_ft,
    p.maximum_building_height_stories AS prev_height_st,    c.maximum_building_height_stories AS curr_height_st,
    p.residential_density AS prev_res_density,              c.residential_density AS curr_res_density,
    p.maximum_residential_units_allowed AS prev_max_units,  c.maximum_residential_units_allowed AS curr_max_units,
    p.calculation_status AS prev_calc_status,               c.calculation_status AS curr_calc_status,
    p.last_updated AS prev_last_updated,                    c.last_updated AS curr_last_updated,

    -- Identity flags
    (p.raw_district_set IS DISTINCT FROM c.raw_district_set)     AS raw_district_chg,
    (p.district_set IS DISTINCT FROM c.district_set)             AS district_chg,
    (p.zid IS DISTINCT FROM c.zid)                               AS zid_chg,
    str_chg(p.zoning_regulation_name, c.zoning_regulation_name)  AS regulation_chg,
    (str_chg(p.zone_district_description, c.zone_district_description)
      OR str_chg(p.planned_development_pd, c.planned_development_pd)
      OR str_chg(p.additional_regulations, c.additional_regulations)) AS text_meta_chg,
    num_chg(p.lot_area_gis, c.lot_area_gis)                      AS lot_area_chg,

    -- Rule flags
    num_chg(p.floor_area_ratio, c.floor_area_ratio)              AS far_chg,
    (num_chg(p.maximum_building_height_feet, c.maximum_building_height_feet)
      OR num_chg(p.maximum_building_height_stories, c.maximum_building_height_stories)) AS height_chg,
    (num_chg(p.residential_density, c.residential_density)
      OR num_chg(p.lodging_density, c.lodging_density))          AS density_chg,
    num_chg(p.maximum_lot_coverage, c.maximum_lot_coverage)      AS lot_coverage_chg,
    num_chg(p.minimum_open_space, c.minimum_open_space)          AS open_space_chg,
    (str_chg(p.minimum_primary_frontage_setback, c.minimum_primary_frontage_setback)
      OR num_chg(p.maximum_primary_frontage_setback, c.maximum_primary_frontage_setback)
      OR str_chg(p.minimum_secondary_frontage_setback, c.minimum_secondary_frontage_setback)
      OR num_chg(p.maximum_secondary_frontage_setback, c.maximum_secondary_frontage_setback)
      OR str_chg(p.minimum_side_setback, c.minimum_side_setback)
      OR num_chg(p.maximum_side_setback, c.maximum_side_setback)
      OR str_chg(p.minimum_rear_setback, c.minimum_rear_setback)
      OR num_chg(p.maximum_rear_setback, c.maximum_rear_setback)
      OR str_chg(p.minimum_water_setback, c.minimum_water_setback)) AS setback_chg,

    -- Derived capacity (moves with rules OR lot area)
    (num_chg(p.maximum_building_footprint, c.maximum_building_footprint)
      OR num_chg(p.maximum_built_area_allowed, c.maximum_built_area_allowed)
      OR num_chg(p.maximum_residential_units_allowed, c.maximum_residential_units_allowed)
      OR num_chg(p.maximum_residential_area_allowed, c.maximum_residential_area_allowed)
      OR num_chg(p.maximum_lodging_rooms_allowed, c.maximum_lodging_rooms_allowed)
      OR num_chg(p.maximum_lodging_area_allowed, c.maximum_lodging_area_allowed)
      OR num_chg(p.maximum_office_area_allowed, c.maximum_office_area_allowed)
      OR num_chg(p.maximum_commercial_area_allowed, c.maximum_commercial_area_allowed)) AS capacity_chg,

    (p.calculation_status IS DISTINCT FROM c.calculation_status) AS calc_status_chg,

    dir(p.floor_area_ratio, c.floor_area_ratio)                          AS far_dir,
    dir(p.maximum_building_height_feet, c.maximum_building_height_feet)  AS height_dir,
    dir(p.residential_density, c.residential_density)                    AS res_density_dir
  FROM prev p
  FULL OUTER JOIN curr c ON p.clip = c.clip
)
SELECT
  *,
  (COALESCE(prev_n_districts, 0) > 1 OR COALESCE(curr_n_districts, 0) > 1) AS split_zoned,
  CASE
    WHEN presence != 'BOTH' THEN presence
    WHEN district_chg       THEN 'DISTRICT_CHANGED'
    WHEN raw_district_chg   THEN 'DISTRICT_FORMAT_ONLY'
    WHEN far_chg OR height_chg OR density_chg OR lot_coverage_chg OR open_space_chg OR setback_chg
                            THEN 'RULES_CHANGED'
    WHEN zid_chg OR regulation_chg OR text_meta_chg THEN 'METADATA_ONLY'
    WHEN lot_area_chg OR capacity_chg               THEN 'LOT_OR_CAPACITY_ONLY'
    WHEN calc_status_chg                            THEN 'CALC_STATUS_ONLY'
    ELSE 'NO_CHANGE'
  END AS change_type,
  CASE
    WHEN presence != 'BOTH' THEN NULL
    WHEN GREATEST(far_dir, height_dir, res_density_dir) = 1
     AND LEAST(far_dir, height_dir, res_density_dir) >= 0 THEN 'UPZONE'
    WHEN LEAST(far_dir, height_dir, res_density_dir) = -1
     AND GREATEST(far_dir, height_dir, res_density_dir) <= 0 THEN 'DOWNZONE'
    WHEN far_dir != 0 OR height_dir != 0 OR res_density_dir != 0 THEN 'MIXED'
    ELSE 'NEUTRAL'
  END AS intensity_direction
FROM joined;