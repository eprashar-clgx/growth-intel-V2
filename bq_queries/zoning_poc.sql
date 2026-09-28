-- ===== STEP 0: key diagnostics (run first) =====
SELECT snapshot, COUNT(*) n_rows, COUNTIF(clip IS NULL) n_null_clip,
  COUNT(DISTINCT clip) n_distinct_clip,
  COUNT(*) - COUNT(DISTINCT clip) n_extra_rows,            -- >0 => dupes / split-zoned
  COUNT(DISTINCT CONCAT(clip,'|',CAST(zid AS STRING))) n_distinct_clip_zid,
  COUNT(DISTINCT fips_code) n_fips,
  COUNTIF(clgx_pmd_actionflag = 'D') n_delete_flag
FROM (
  SELECT 'PREV' snapshot, clip, zid, fips_code, clgx_pmd_actionflag
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_zoning_hist.gridics_zoning_archive_250903`
  UNION ALL
  SELECT 'CURR', clip, zid, fips_code, clgx_pmd_actionflag FROM `<<CURR_TABLE>>`
) GROUP BY snapshot;

-- ===== STEP 1: per-CLIP change table =====
CREATE TEMP FUNCTION str_chg(a STRING, b STRING) AS (
  COALESCE(UPPER(TRIM(a)),'') != COALESCE(UPPER(TRIM(b)),''));
CREATE TEMP FUNCTION num_chg(a FLOAT64, b FLOAT64) AS (
  (a IS NULL) != (b IS NULL)
  OR (a IS NOT NULL AND ABS(a-b) > 1e-6 * GREATEST(1, ABS(a), ABS(b))));
CREATE TEMP FUNCTION dir(a FLOAT64, b FLOAT64) AS (
  IF(a IS NULL OR b IS NULL OR NOT num_chg(a,b), 0, IF(b > a, 1, -1)));

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v1` AS
WITH prev AS (
  SELECT *, COUNT(*) OVER (PARTITION BY clip) n_rows_for_clip
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_zoning_hist.gridics_zoning_archive_250903`
  WHERE clip IS NOT NULL AND COALESCE(clgx_pmd_actionflag,'') != 'D'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY clip ORDER BY clgx_pmd_load_timestamp DESC, zid) = 1
),
curr AS (
  SELECT *, COUNT(*) OVER (PARTITION BY clip) n_rows_for_clip
  FROM `<<CURR_TABLE>>`
  WHERE clip IS NOT NULL AND COALESCE(clgx_pmd_actionflag,'') != 'D'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY clip ORDER BY clgx_pmd_load_timestamp DESC, zid) = 1
),
joined AS (
  SELECT
    COALESCE(c.clip, p.clip) clip,
    COALESCE(c.fips_code, p.fips_code) fips_code,
    COALESCE(c.state, p.state) state,
    COALESCE(c.market_name, p.market_name) market_name,
    CASE WHEN p.clip IS NULL THEN 'ADDED' WHEN c.clip IS NULL THEN 'REMOVED' ELSE 'BOTH' END presence,
    p.n_rows_for_clip prev_n_rows, c.n_rows_for_clip curr_n_rows,
    p.zid prev_zid, c.zid curr_zid,
    p.zoning_district prev_district, c.zoning_district curr_district,
    p.zone_district_description prev_desc, c.zone_district_description curr_desc,
    p.floor_area_ratio prev_far, c.floor_area_ratio curr_far,
    p.maximum_building_height_feet prev_height_ft, c.maximum_building_height_feet curr_height_ft,
    p.residential_density prev_res_density, c.residential_density curr_res_density,
    p.lot_area_gis prev_lot_area, c.lot_area_gis curr_lot_area,

    (p.zoning_district IS DISTINCT FROM c.zoning_district) raw_district_chg,
    str_chg(p.zoning_district, c.zoning_district) district_chg,
    (p.zid IS DISTINCT FROM c.zid) zid_chg,
    (str_chg(p.zone_district_description, c.zone_district_description)
      OR str_chg(p.zoning_regulation_name, c.zoning_regulation_name)
      OR str_chg(p.planned_development_pd, c.planned_development_pd)
      OR str_chg(p.additional_regulations, c.additional_regulations)) text_meta_chg,
    num_chg(p.lot_area_gis, c.lot_area_gis) lot_area_chg,

    num_chg(p.floor_area_ratio, c.floor_area_ratio) far_chg,
    (num_chg(p.maximum_building_height_feet, c.maximum_building_height_feet)
      OR num_chg(p.maximum_building_height_stories, c.maximum_building_height_stories)) height_chg,
    (num_chg(p.residential_density, c.residential_density)
      OR num_chg(p.lodging_density, c.lodging_density)) density_chg,
    num_chg(p.maximum_lot_coverage, c.maximum_lot_coverage) lot_coverage_chg,
    num_chg(p.minimum_open_space, c.minimum_open_space) open_space_chg,
    (str_chg(p.minimum_primary_frontage_setback, c.minimum_primary_frontage_setback)
      OR num_chg(p.maximum_primary_frontage_setback, c.maximum_primary_frontage_setback)
      OR str_chg(p.minimum_secondary_frontage_setback, c.minimum_secondary_frontage_setback)
      OR num_chg(p.maximum_secondary_frontage_setback, c.maximum_secondary_frontage_setback)
      OR str_chg(p.minimum_side_setback, c.minimum_side_setback)
      OR num_chg(p.maximum_side_setback, c.maximum_side_setback)
      OR str_chg(p.minimum_rear_setback, c.minimum_rear_setback)
      OR num_chg(p.maximum_rear_setback, c.maximum_rear_setback)
      OR str_chg(p.minimum_water_setback, c.minimum_water_setback)) setback_chg,

    (num_chg(p.maximum_building_footprint, c.maximum_building_footprint)
      OR num_chg(p.maximum_built_area_allowed, c.maximum_built_area_allowed)
      OR num_chg(p.maximum_residential_units_allowed, c.maximum_residential_units_allowed)
      OR num_chg(p.maximum_residential_area_allowed, c.maximum_residential_area_allowed)
      OR num_chg(p.maximum_lodging_rooms_allowed, c.maximum_lodging_rooms_allowed)
      OR num_chg(p.maximum_lodging_area_allowed, c.maximum_lodging_area_allowed)
      OR num_chg(p.maximum_office_area_allowed, c.maximum_office_area_allowed)
      OR num_chg(p.maximum_commercial_area_allowed, c.maximum_commercial_area_allowed)) capacity_chg,

    (p.calculation_status IS DISTINCT FROM c.calculation_status) calc_status_chg,
    dir(p.floor_area_ratio, c.floor_area_ratio) far_dir,
    dir(p.maximum_building_height_feet, c.maximum_building_height_feet) height_dir,
    dir(p.residential_density, c.residential_density) res_density_dir
  FROM prev p FULL OUTER JOIN curr c ON p.clip = c.clip
)
SELECT *,
  CASE
    WHEN presence != 'BOTH' THEN presence
    WHEN district_chg THEN 'DISTRICT_CHANGED'
    WHEN raw_district_chg THEN 'DISTRICT_FORMAT_ONLY'
    WHEN far_chg OR height_chg OR density_chg OR lot_coverage_chg OR open_space_chg OR setback_chg
      THEN 'RULES_CHANGED'
    WHEN zid_chg OR text_meta_chg THEN 'METADATA_ONLY'
    WHEN lot_area_chg OR capacity_chg THEN 'LOT_OR_CAPACITY_ONLY'
    WHEN calc_status_chg THEN 'CALC_STATUS_ONLY'
    ELSE 'NO_CHANGE'
  END change_type,
  CASE
    WHEN presence != 'BOTH' THEN NULL
    WHEN GREATEST(far_dir,height_dir,res_density_dir) = 1 AND LEAST(far_dir,height_dir,res_density_dir) >= 0 THEN 'UPZONE'
    WHEN LEAST(far_dir,height_dir,res_density_dir) = -1 AND GREATEST(far_dir,height_dir,res_density_dir) <= 0 THEN 'DOWNZONE'
    WHEN far_dir != 0 OR height_dir != 0 OR res_density_dir != 0 THEN 'MIXED'
    ELSE 'NEUTRAL'
  END intensity_direction
FROM joined;

-- ===== STEP 2: summaries =====
SELECT change_type, intensity_direction, COUNT(*) n_clips,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) pct, COUNT(DISTINCT fips_code) n_fips
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v1`
GROUP BY 1, 2 ORDER BY n_clips DESC;

SELECT fips_code, prev_district, curr_district, COUNT(*) n_clips
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v1`
WHERE change_type = 'DISTRICT_CHANGED'
GROUP BY 1, 2, 3 ORDER BY n_clips DESC LIMIT 100;