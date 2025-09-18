-- Create base for Growth Intelligence V1 for analysis
CREATE OR REPLACE TABLE work_eprashar.growth_intelligence_base_qgis AS
WITH GI AS (
  SELECT 
    puid,
    previous_land_use_code,
    landuse_change_indicator,
    CASE
      WHEN recent_new_construction_sale_indicator = 'Y' OR recent_yearbuilt_indicator = 'Y' THEN 'Recently Completed'
      WHEN recent_new_con_bldg_permit_indicator= 'Y' THEN 'Ongoing Growth' -- old definition: AND with builder_developer_own_ind = 'Y'
      WHEN landuse_change_indicator='Y' or builder_developer_ownership_indicator='Y' THEN 'Early Growth'
      WHEN new_clip_indicator = 'Y' THEN 'New Clip'
    END AS growth_stage,  
    builder_developer_ownership_indicator,
    recent_new_con_bldg_permit_indicator, 
    recent_yearbuilt_indicator,
    recent_new_construction_sale_indicator, 
    new_clip_indicator
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_enriched.vw_edr_panoramiq_growth_indicators`
),
parcel_counts AS (
  SELECT
    clip,
    COUNT(geometry) AS geom_counts
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_parcel_polygons.property_parcelpolygon`
  GROUP BY clip
),
-- While panoramiq_growth_indicators doesn't have geometry,
-- we can discuss if this logic can be leveraged to prioritize polygon / filter out point geometries
parcel_geom AS (
  SELECT
    clip,
    geometry
  FROM (
    -- 1) Prioritize polygon geometry over point geometry
    SELECT
      clip,
      geometry,
      ROW_NUMBER() OVER(PARTITION BY clip ORDER BY CASE WHEN ST_GEOMETRYTYPE(geometry) IN ('ST_Polygon', 'ST_MultiPolygon') THEN 1 ELSE 2 END) as rn
    FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_parcel_polygons.property_parcelpolygon`
  )
  WHERE rn = 1
),
sp_owners AS (
  SELECT  
    fips,  
    puid,
    ownr1fullname,
    irislandusecd,
    irislandusecddesc,
    ROUND(sumareabldg / landdimsqfttotal, 2) as blgd_land_ratio
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_tax_assessor.property_v1`
),
owner_classification AS (
  -- A name could have multiple classifications tagged to it 
  -- Even the same classification for the exact same name is done multiple times (example PULTE HOMES LLC) appears twice because two different people
  -- added it to the database (!!)
  -- FIX: De-duplicate and prioritize owner classifications more robustly.
  SELECT
    name_desc,
    name_classification
  FROM (
    SELECT
      cddesc AS name_desc,
      cdtbl AS name_classification,
      -- This logic prioritizes the 'BLDRC' classification first,
      -- and then picks the most recent record if multiple BLDRC records exist.
      ROW_NUMBER() OVER(
        PARTITION BY cddesc 
        ORDER BY 
          CASE WHEN cdtbl = 'BLDRC' THEN 1 ELSE 2 END ASC, -- Prioritize BLDRC
          updatetimestamp DESC -- Then pick the most recent record
      ) as rn
    FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_refint.vw_codetable`
  )
  WHERE rn = 1
),
sp_address AS (
  SELECT 
    clip, 
    AVG(DELIVERY_POINT_OCCUPANCY) as addr_vacant_ratio,
    -- 2) Pick any one address for each clip
    ANY_VALUE(STD_MATCH_ADDRESS_BASE) as std_address
  FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.edr_sdp_address_connect`
  GROUP BY clip  
),
-- Check if num_structures already exists in schema
-- If not, we can add COALESCE(COUNT(yybltactdt), COUNT(yyblteffdt)) AS num_structures
structure_info AS (
  SELECT
    puid,
    -- 3) Pick the latest value for each year column
    MAX(yybltactdt) AS latest_year_built,
    MAX(yyblteffdt) AS latest_eff_year_built,
    -- 3) Also get the counts of each column
    COUNT(yybltactdt) AS count_year_built,
    COUNT(yyblteffdt) AS count_eff_year_built
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_fulfillment.vw_structure_v1`
  GROUP BY puid
)

-- 4) Final SELECT and JOINs
SELECT 
  gi.puid,
  LEFT(spo.fips, 2) AS state_abbr,
  spo.fips,
  gi.previous_land_use_code,
  spo.irislandusecd,
  spo.irislandusecddesc,
  gi.landuse_change_indicator,
  gi.growth_stage,  
  gi.builder_developer_ownership_indicator,
  gi.recent_new_con_bldg_permit_indicator, 
  gi.recent_yearbuilt_indicator,
  gi.recent_new_construction_sale_indicator, 
  gi.new_clip_indicator,
  pc.geom_counts,
  spo.ownr1fullname,
  oc.name_classification, -- Join to the new, de-duplicated classification table
  spo.blgd_land_ratio,
  spa.addr_vacant_ratio,
  spa.std_address,
  si.latest_year_built,
  si.latest_eff_year_built,
  si.count_year_built,
  si.count_eff_year_built,
  pg.geometry
FROM GI AS gi
LEFT JOIN parcel_geom AS pg
  ON CAST(gi.puid AS STRING) = pg.clip
LEFT JOIN parcel_counts AS pc
  ON CAST(gi.puid AS STRING) = pc.clip
LEFT JOIN sp_owners AS spo
  ON gi.puid = spo.puid
LEFT JOIN sp_address AS spa
  ON gi.puid = spa.clip
LEFT JOIN structure_info AS si
  ON gi.puid = si.puid
LEFT JOIN owner_classification AS oc -- Join to the new, de-duplicated classification table
  ON spo.ownr1fullname = oc.name_desc;
