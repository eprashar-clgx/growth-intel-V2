-- Zoning change MVP1 vs MVP2 -> parcel polygons for CARTO (Maricopa AZ), one row per CLIP (#33)
-- Run after zoning_change_compare.sql. Needs prd access (parcel_universe).
-- parcel_shape_id comes from the comparison table (one per CLIP, same in both versions: C1 n_parcel_mismatch = 0),
-- so vw_clip_to_parcel isn't re-joined. Style by zoning_change_class_mvp1 / _mvp2 or filter to is_class_changed.
-- If parcel_polygon is a STRING, wrap it in SAFE.ST_GEOGFROMTEXT(...) (or SAFE.ST_GEOGFROMGEOJSON for GeoJSON)
DECLARE county_name STRING DEFAULT 'Maricopa AZ';

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_compare_parcels` AS
WITH
z AS (
  SELECT * FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_compare`
  WHERE county = county_name
),
pu AS (
  SELECT CAST(parcel_shape_id AS STRING) AS parcel_shape_id, ANY_VALUE(parcel_polygon) AS parcel_polygon
  FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.parcel_universe`
  WHERE CAST(parcel_shape_id AS STRING) IN (SELECT parcel_shape_id FROM z)
  GROUP BY 1
)
SELECT
  z.clip, z.parcel_shape_id, z.in_mvp1, z.in_mvp2,
  z.curr_reg, z.transition, z.prev_desc, z.curr_desc, z.transition_desc,
  z.zoning_change_class_mvp1, z.zoning_change_class_mvp2,
  z.outcome_group_mvp1, z.outcome_group_mvp2,
  z.is_class_changed, z.is_group_changed,
  z.growth_score, z.growth_confidence, z.growth_score_inputs,
  z.change_pattern, z.has_owner_evidence, z.has_land_evidence, z.yb_status, z.patch_id, z.patch_n,
  z.k_same_tr_share, z.n_same_tr_radius,
  z.is_partial_rezone, z.is_reg_move, z.is_growth_clip,
  z.owner_name, z.max_yr_built,
  z.is_super_parcel, z.sp_id, z.sp_owner_name,
  IF(z.parcel_shape_id IS NULL, NULL, COUNT(*) OVER (PARTITION BY z.parcel_shape_id)) AS n_clips_on_parcel,
  STARTS_WITH(z.curr_dset, 'T') AS is_mesa_transect,
  pu.parcel_polygon AS geom,
  COALESCE(z.geom, ST_CENTROID(pu.parcel_polygon)) AS point_geom
FROM z
LEFT JOIN pu USING (parcel_shape_id);
