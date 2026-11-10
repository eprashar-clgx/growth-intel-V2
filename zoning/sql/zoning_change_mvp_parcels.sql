-- Zoning change MVP -> parcel polygons for CARTO (Maricopa)
-- Run after zoning_change_mvp.sql
-- If parcel_polygon is a STRING, wrap it in SAFE.ST_GEOGFROMTEXT(...) (or SAFE.ST_GEOGFROMGEOJSON for GeoJSON)
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp_parcels` AS
WITH z AS (
  SELECT * EXCEPT (parcel_shape_id) FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp`
  WHERE county = 'Maricopa AZ'
    AND zoning_change_class IN ('NEEDLE_PARCEL','SMALL_TRACT','MASS_CHANGE','SLIVER_DROP','CITY_PROGRAM','NO_COORDS')
),
c2p AS (
  SELECT DISTINCT CAST(clip AS STRING) AS clip, parcel_shape_id
  FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.vw_clip_to_parcel`
  WHERE CAST(clip AS STRING) IN (SELECT clip FROM z)
),
pu AS (
  SELECT parcel_shape_id, ANY_VALUE(parcel_polygon) AS parcel_polygon
  FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.parcel_universe`
  WHERE parcel_shape_id IN (SELECT parcel_shape_id FROM c2p)
  GROUP BY 1
)
SELECT
  z.clip, c2p.parcel_shape_id, z.curr_reg, z.transition,
  z.prev_desc, z.curr_desc, z.transition_desc,
  z.zoning_change_class,
  z.is_growth_clip, z.is_partial_rezone, z.k_same_tr_share, z.n_same_tr_radius,
  z.owner_name, z.max_yr_built,
  z.is_super_parcel, z.sp_id, z.sp_owner_name,
  COUNT(*) OVER (PARTITION BY c2p.parcel_shape_id) AS n_clips_on_parcel,
  STARTS_WITH(z.curr_dset, 'T') AS is_mesa_transect,
  pu.parcel_polygon AS geom,
  COALESCE(z.geom, ST_CENTROID(pu.parcel_polygon)) AS point_geom
FROM z
LEFT JOIN c2p USING (clip)
LEFT JOIN pu USING (parcel_shape_id);
