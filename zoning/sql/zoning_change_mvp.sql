-- Zoning change MVP: rules R1-R4, R6-R13 (see zoning/docs/zoning_POC.md)
-- Output: one row per CLIP whose zoning district changed, with its change class, a growth table flag, owner name + year built from parcel essentials, and the super parcel flag, ID and owner name
DECLARE fips_list        ARRAY<STRING> DEFAULT ['04013', '12001'];  -- Maricopa AZ, Alachua FL
DECLARE min_group_n      INT64   DEFAULT 20;    -- R8, R9
DECLARE regime_chg_share FLOAT64 DEFAULT 0.5;   -- R8
DECLARE rename_share     FLOAT64 DEFAULT 0.9;   -- R9
DECLARE prog_min_src     INT64   DEFAULT 4;     -- R11
DECLARE prog_min_places  INT64   DEFAULT 10;    -- R11
DECLARE prog_max_rural   FLOAT64 DEFAULT 0.5;   -- R11
DECLARE radius_m         FLOAT64 DEFAULT 1000;  -- R12, R13
DECLARE knn_k            INT64   DEFAULT 20;    -- R12
DECLARE needle_max_share FLOAT64 DEFAULT 0.15;  -- R12
DECLARE tract_max_clips  INT64   DEFAULT 50;    -- R13

-- R2: strip punctuation/whitespace, uppercase
CREATE TEMP FUNCTION dcode(x STRING) AS (
  NULLIF(UPPER(REGEXP_REPLACE(x, r'[^A-Za-z0-9]', '')), '')
);
-- every district in set a is also in set b
CREATE TEMP FUNCTION is_sub(a STRING, b STRING) AS (
  NOT EXISTS (SELECT 1 FROM UNNEST(SPLIT(a, '|')) AS x WHERE x NOT IN UNNEST(SPLIT(b, '|')))
);
-- R11 helper: agricultural / rural source districts (heuristic)
CREATE TEMP FUNCTION is_rural(dset STRING) AS (
  REGEXP_CONTAINS(dset, r'(^|\|)(AG[A-Z0-9]*|A[0-9]*|AR|RA|RU[0-9]*|RURAL[A-Z0-9]*)(\||$)')
);

-- R1-R3: PREV snapshot -> one row per CLIP with its normalized district set
CREATE TEMP TABLE prev_s AS
SELECT
  clip,
  ANY_VALUE(fips) AS fips,
  IFNULL(MIN(reg), '(none)') AS reg,
  STRING_AGG(DISTINCT d, '|' ORDER BY d) AS dset,
  STRING_AGG(DISTINCT dd, ' / ' ORDER BY dd) AS dset_desc
FROM (
  SELECT
    CAST(clip AS STRING) AS clip,
    LPAD(fips_code, 5, '0') AS fips,
    NULLIF(TRIM(zoning_regulation_name), '') AS reg,
    dcode(zoning_district) AS d,
    NULLIF(TRIM(zone_district_description), '') AS dd
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_zoning_hist.gridics_zoning_archive_250903`
  WHERE LPAD(fips_code, 5, '0') IN UNNEST(fips_list)
    AND clip IS NOT NULL
    AND COALESCE(clgx_pmd_actionflag, '') != 'D'
)
GROUP BY clip;

-- R1-R3: CURR snapshot (+ coordinates)
CREATE TEMP TABLE curr_s AS
SELECT
  clip,
  ANY_VALUE(fips) AS fips,
  IFNULL(MIN(reg), '(none)') AS reg,
  STRING_AGG(DISTINCT d, '|' ORDER BY d) AS dset,
  STRING_AGG(DISTINCT dd, ' / ' ORDER BY dd) AS dset_desc,
  ANY_VALUE(lat) AS lat,
  ANY_VALUE(lon) AS lon
FROM (
  SELECT
    CAST(clip AS STRING) AS clip,
    LPAD(fips_code, 5, '0') AS fips,
    NULLIF(TRIM(zoning_regulation_name), '') AS reg,
    dcode(zoning_district) AS d,
    NULLIF(TRIM(zone_district_description), '') AS dd,
    IF(clip_latitude BETWEEN -90 AND 90 AND clip_longitude BETWEEN -180 AND 180
       AND clip_latitude != 0, clip_latitude, NULL) AS lat,
    IF(clip_latitude BETWEEN -90 AND 90 AND clip_longitude BETWEEN -180 AND 180
       AND clip_latitude != 0, clip_longitude, NULL) AS lon
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_zoning.gridics_zoning`
  WHERE LPAD(fips_code, 5, '0') IN UNNEST(fips_list)
    AND clip IS NOT NULL
    AND COALESCE(clgx_pmd_actionflag, '') != 'D'
)
GROUP BY clip;

-- R4 (CLIP in both snapshots) + R6 (zoning present on both sides)
CREATE TEMP TABLE j AS
SELECT
  c.clip, c.fips,
  p.reg AS prev_reg, c.reg AS curr_reg,
  p.dset AS prev_dset, c.dset AS curr_dset,
  p.dset_desc AS prev_desc, c.dset_desc AS curr_desc,
  c.lat, c.lon,
  IF(c.lat IS NULL OR c.lon IS NULL, NULL, ST_GEOGPOINT(c.lon, c.lat)) AS geog
FROM prev_s AS p
JOIN curr_s AS c USING (clip)
WHERE p.dset IS NOT NULL AND c.dset IS NOT NULL;

-- R7 (district set changed) -> R8 (code regime switch) -> R9 (rename/merge) -> R10 (sliver drop)
CREATE TEMP TABLE chg AS
WITH
reg_stats AS (
  SELECT fips, prev_reg,
    COUNT(*) AS reg_n,
    AVG(IF(prev_dset != curr_dset, 1, 0)) AS reg_chg_share
  FROM j
  GROUP BY 1, 2
),
rename_map AS (
  SELECT fips, prev_reg, prev_dset AS from_d, curr_dset AS to_d
  FROM (
    SELECT fips, prev_reg, prev_dset, curr_dset,
      COUNT(*) AS n,
      SUM(COUNT(*)) OVER (PARTITION BY fips, prev_reg, prev_dset) AS tot
    FROM j
    WHERE STRPOS(prev_dset, '|') = 0 AND STRPOS(curr_dset, '|') = 0
    GROUP BY 1, 2, 3, 4
  )
  WHERE tot >= min_group_n AND n / tot >= rename_share AND prev_dset != curr_dset
),
changed AS (
  SELECT * FROM j WHERE prev_dset != curr_dset
),
mapped AS (
  SELECT clip, STRING_AGG(DISTINCT md, '|' ORDER BY md) AS prev_dset_mapped
  FROM (
    SELECT c.clip, COALESCE(m.to_d, e) AS md
    FROM changed AS c
    CROSS JOIN UNNEST(SPLIT(c.prev_dset, '|')) AS e
    LEFT JOIN rename_map AS m
      ON m.fips = c.fips AND m.prev_reg = c.prev_reg AND m.from_d = e
  )
  GROUP BY clip
)
SELECT
  c.*,
  m.prev_dset_mapped,
  rs.reg_n,
  rs.reg_chg_share,
  (is_sub(m.prev_dset_mapped, c.curr_dset) AND m.prev_dset_mapped != c.curr_dset) AS is_partial_rezone,
  (c.prev_reg != c.curr_reg) AS is_reg_move,
  CASE
    WHEN c.prev_reg != c.curr_reg
         AND rs.reg_n >= min_group_n
         AND rs.reg_chg_share >= regime_chg_share   THEN 'CODE_REGIME_SWITCH'
    WHEN m.prev_dset_mapped = c.curr_dset           THEN 'RENAME_OR_MERGE'
    WHEN is_sub(c.curr_dset, m.prev_dset_mapped)    THEN 'SLIVER_DROP'
    ELSE 'CANDIDATE'
  END AS stage1
FROM changed AS c
JOIN mapped AS m USING (clip)
JOIN reg_stats AS rs ON rs.fips = c.fips AND rs.prev_reg = c.prev_reg;

-- R11: city program (destination district handed out across the jurisdiction)
CREATE TEMP TABLE prog AS
WITH
cand AS (
  SELECT * FROM chg WHERE stage1 = 'CANDIDATE'
),
prev_universe AS (
  SELECT DISTINCT fips, e AS d
  FROM prev_s, UNNEST(SPLIT(dset, '|')) AS e
  WHERE dset IS NOT NULL
),
cl AS (
  SELECT fips, curr_reg, curr_dset, prev_dset,
    ST_CLUSTERDBSCAN(geog, 300, 1) OVER (PARTITION BY fips, curr_reg, curr_dset) AS cl_id
  FROM cand
  WHERE geog IS NOT NULL
),
tgt AS (
  SELECT fips, curr_reg, curr_dset,
    COUNT(DISTINCT prev_dset) AS tgt_n_src,
    COUNT(DISTINCT cl_id)     AS tgt_n_places,
    AVG(IF(is_rural(prev_dset), 1, 0)) AS tgt_rural_src_share
  FROM cl
  GROUP BY 1, 2, 3
)
SELECT
  c.clip,
  t.tgt_n_src, t.tgt_n_places, t.tgt_rural_src_share,
  (STRPOS(c.curr_dset, '|') = 0 AND pu.d IS NULL
    AND NOT REGEXP_CONTAINS(c.curr_dset, r'^(PD|PAD|PUD|PCD|PDD)')) AS tgt_is_new_district,
  COALESCE(
    (STRPOS(c.curr_dset, '|') = 0 AND pu.d IS NULL
      AND NOT REGEXP_CONTAINS(c.curr_dset, r'^(PD|PAD|PUD|PCD|PDD)'))
    OR (t.tgt_n_src >= prog_min_src
        AND t.tgt_n_places >= prog_min_places
        AND t.tgt_rural_src_share < prog_max_rural),
    FALSE) AS is_city_program
FROM cand AS c
LEFT JOIN tgt AS t USING (fips, curr_reg, curr_dset)
LEFT JOIN prev_universe AS pu ON pu.fips = c.fips AND pu.d = c.curr_dset;

-- R12 (kNN isolation) + R13 (tract size)
CREATE TEMP TABLE knn AS
WITH
cand AS (
  SELECT c.clip, c.geog, c.prev_dset, c.curr_dset
  FROM chg AS c
  JOIN prog AS p USING (clip)
  WHERE c.geog IS NOT NULL AND NOT p.is_city_program
),
nbr AS (
  SELECT
    c.clip,
    ST_DISTANCE(c.geog, n.geog) AS d,
    (n.prev_dset = c.prev_dset AND n.curr_dset = c.curr_dset) AS same_tr
  FROM cand AS c
  JOIN j AS n ON ST_DWITHIN(c.geog, n.geog, radius_m)
  WHERE n.clip != c.clip AND n.geog IS NOT NULL
),
ranked AS (
  SELECT clip, same_tr, ROW_NUMBER() OVER (PARTITION BY clip ORDER BY d) AS rk
  FROM nbr
  WHERE d > 1                                   -- exclude co-located (condos)
),
k AS (
  SELECT clip,
    COUNTIF(rk <= knn_k)             AS k_n,
    COUNTIF(rk <= knn_k AND same_tr) AS k_same_tr
  FROM ranked
  GROUP BY clip
),
r AS (
  SELECT clip,
    COUNTIF(d <= 1)  AS n_colocated,
    COUNTIF(same_tr) AS n_same_tr_radius
  FROM nbr
  GROUP BY clip
)
SELECT
  c.clip,
  k.k_n,
  k.k_same_tr,
  SAFE_DIVIDE(k.k_same_tr, k.k_n) AS k_same_tr_share,
  IFNULL(r.n_colocated, 0)        AS n_colocated,
  IFNULL(r.n_same_tr_radius, 0)   AS n_same_tr_radius
FROM cand AS c
LEFT JOIN k USING (clip)
LEFT JOIN r USING (clip);

-- Final table: change class + growth table flag + parcel essentials attributes + super parcel
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp` AS
WITH growth AS (
  SELECT DISTINCT CAST(puid AS STRING) AS clip
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_enriched.vw_edr_panoramiq_growth_indicators_v2`
  WHERE CAST(puid AS STRING) IN (SELECT clip FROM chg)
),
-- one parcel per CLIP; a parcel can hold several CLIPs (condos)
c2p AS (
  SELECT CAST(clip AS STRING) AS clip, MIN(CAST(parcel_shape_id AS STRING)) AS parcel_shape_id
  FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.vw_clip_to_parcel`
  WHERE CAST(clip AS STRING) IN (SELECT clip FROM chg)
  GROUP BY 1
),
-- parcel essentials can repeat a parcel_shape_id: dedupe to one row per parcel
pe AS (
  SELECT
    parcel_shape_id,
    ARRAY_AGG(ownr1fullname IGNORE NULLS ORDER BY src_record_update_date DESC, clip LIMIT 1)[SAFE_OFFSET(0)] AS owner_name,
    COALESCE(MAX(YyBltActDt), MAX(YyBltEffDt)) AS max_yr_built
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_fulfillment_hist.parcel_essentials_dsf`
  WHERE parcel_shape_id IN (SELECT parcel_shape_id FROM c2p)
  GROUP BY 1
),
-- super parcel per parcel: use the vw_ views, which are one sp_id per parcel and one row per sp_id
-- (the base lkp_superparcels / superparcels tables repeat keys)
sp AS (
  SELECT CAST(l.parcel_shape_id AS STRING) AS parcel_shape_id, l.sp_id, s.owner AS sp_owner_name
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_property_graph.vw_lkp_superparcels` AS l
  LEFT JOIN `clgx-idap-bigquery-prd-a990.edr_ent_property_property_graph.vw_superparcels` AS s USING (sp_id)
  WHERE CAST(l.parcel_shape_id AS STRING) IN (SELECT parcel_shape_id FROM c2p)
)
SELECT
  c.clip,
  c.fips,
  CASE c.fips WHEN '04013' THEN 'Maricopa AZ' WHEN '12001' THEN 'Alachua FL' ELSE c.fips END AS county,
  c.prev_reg, c.curr_reg,
  c.prev_dset, c.curr_dset, c.prev_dset_mapped,
  CONCAT(c.prev_dset, ' → ', c.curr_dset) AS transition,
  c.prev_desc, c.curr_desc,
  CONCAT(IFNULL(c.prev_desc, '?'), ' → ', IFNULL(c.curr_desc, '?')) AS transition_desc,
  CASE
    WHEN c.stage1 != 'CANDIDATE'                                   THEN c.stage1
    WHEN p.is_city_program                                         THEN 'CITY_PROGRAM'
    WHEN c.geog IS NULL                                            THEN 'NO_COORDS'
    WHEN COALESCE(k.k_same_tr_share, 0) <= needle_max_share        THEN 'NEEDLE_PARCEL'
    WHEN k.n_same_tr_radius + 1 <= tract_max_clips                 THEN 'SMALL_TRACT'
    ELSE 'MASS_CHANGE'
  END AS zoning_change_class,
  c.is_partial_rezone,
  c.is_reg_move,
  (g.clip IS NOT NULL) AS is_growth_clip,
  m.parcel_shape_id,
  pe.owner_name,
  pe.max_yr_built,
  (sp.sp_id IS NOT NULL) AS is_super_parcel,
  sp.sp_id,
  sp.sp_owner_name,
  c.reg_n, c.reg_chg_share,
  p.tgt_is_new_district, p.tgt_n_src, p.tgt_n_places, p.tgt_rural_src_share,
  k.k_n, k.k_same_tr, k.k_same_tr_share, k.n_colocated, k.n_same_tr_radius,
  c.lat, c.lon,
  c.geog AS geom
FROM chg AS c
LEFT JOIN prog   AS p USING (clip)
LEFT JOIN knn    AS k USING (clip)
LEFT JOIN growth AS g USING (clip)
LEFT JOIN c2p    AS m USING (clip)
LEFT JOIN pe          USING (parcel_shape_id)
LEFT JOIN sp          ON sp.parcel_shape_id = m.parcel_shape_id;

-- S1: change class x growth table coverage
SELECT
  county,
  zoning_change_class,
  COUNT(*)                AS n_clips,
  COUNTIF(is_growth_clip) AS n_in_growth,
  COUNTIF(NOT is_growth_clip) AS n_not_in_growth,
  ROUND(100 * COUNTIF(is_growth_clip) / COUNT(*), 1) AS pct_in_growth
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp`
GROUP BY 1, 2
ORDER BY county, n_clips DESC;

-- S2: top transitions per change class (sanity check)
SELECT
  county,
  zoning_change_class,
  COUNT(*) AS n_clips,
  APPROX_TOP_COUNT(transition, 5) AS top_transitions
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp`
GROUP BY 1, 2
ORDER BY county, n_clips DESC;

-- S3: parcel essentials match rate (CLIP -> parcel -> owner name / year built)
SELECT
  county,
  COUNT(*)                                  AS n_clips,
  COUNT(DISTINCT clip)                      AS n_distinct_clips,
  ROUND(100 * COUNTIF(parcel_shape_id IS NOT NULL) / COUNT(*), 1) AS pct_with_parcel,
  ROUND(100 * COUNTIF(owner_name IS NOT NULL) / COUNT(*), 1)      AS pct_with_owner_name,
  ROUND(100 * COUNTIF(max_yr_built IS NOT NULL) / COUNT(*), 1)    AS pct_with_yr_built,
  COUNTIF(max_yr_built = 0)                 AS n_yr_built_zero
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp`
GROUP BY 1
ORDER BY 1;

-- S4: change class x super parcel (R12/R13 impact: do super parcels inflate n_same_tr_radius?)
SELECT
  county,
  zoning_change_class,
  COUNT(*)                                                   AS n_clips,
  COUNTIF(is_super_parcel)                                   AS n_super_parcel,
  ROUND(100 * COUNTIF(is_super_parcel) / COUNT(*), 1)        AS pct_super_parcel,
  COUNT(DISTINCT sp_id)                                      AS n_distinct_sp,
  ROUND(AVG(IF(is_super_parcel, n_same_tr_radius, NULL)), 1)     AS avg_same_tr_radius_sp,
  ROUND(AVG(IF(NOT is_super_parcel, n_same_tr_radius, NULL)), 1) AS avg_same_tr_radius_non_sp
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp`
GROUP BY 1, 2
ORDER BY county, n_clips DESC;

-- S5 (R11): per-target diagnostic. A target is (county, curr_reg, curr_dset); R11 stats are per target.
-- Shows targets labelled CITY_PROGRAM, near misses (>= 3 source district sets) and the Mesa review targets.
-- Literals mirror the R11 DECLAREs (prog_min_src 4, prog_max_rural 0.5) so this runs standalone.
SELECT
  county,
  curr_reg,
  curr_dset,
  COUNT(*)                                           AS n_clips,
  COUNTIF(geom IS NOT NULL)                          AS n_with_coords,
  LOGICAL_OR(tgt_is_new_district)                    AS tgt_is_new_district,
  MAX(tgt_n_src)                                     AS tgt_n_src,
  MAX(tgt_n_places)                                  AS tgt_n_places,
  ROUND(COUNTIF(geom IS NOT NULL) / MAX(tgt_n_places), 1) AS clips_per_place,
  ROUND(MAX(tgt_rural_src_share), 2)                 AS tgt_rural_src_share,
  ROUND(ST_AREA(ST_CONVEXHULL(ST_UNION_AGG(geom))) / 1e6, 2) AS hull_km2,
  COALESCE(MAX(tgt_n_src) >= 4 AND MAX(tgt_n_places) >= 10 AND MAX(tgt_rural_src_share) < 0.5, FALSE) AS passes_wide_10,
  COALESCE(MAX(tgt_n_src) >= 4 AND MAX(tgt_n_places) >= 25 AND MAX(tgt_rural_src_share) < 0.5, FALSE) AS passes_wide_25,
  APPROX_TOP_COUNT(zoning_change_class, 6)           AS classes,
  APPROX_TOP_COUNT(prev_dset, 6)                     AS top_src,
  COUNT(DISTINCT owner_name)                         AS n_distinct_owner,
  COUNT(DISTINCT sp_id)                              AS n_distinct_sp
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp`
WHERE tgt_is_new_district IS NOT NULL  -- R11 input: stage1 = 'CANDIDATE'
GROUP BY 1, 2, 3
HAVING LOGICAL_OR(zoning_change_class = 'CITY_PROGRAM')
  OR tgt_n_src >= 3  -- SELECT alias (= MAX(tgt_n_src)); HAVING resolves aliases first
  OR (county = 'Maricopa AZ' AND curr_dset IN ('RM4', 'T4N', 'T4NF', 'T3N', 'T6MS'))
ORDER BY county, tgt_n_places DESC, n_clips DESC;

-- S6 (R11): sensitivity of CITY_PROGRAM to prog_min_places (other thresholds as declared)
WITH t AS (
  SELECT
    county, curr_reg, curr_dset,
    COUNT(*)                        AS n_clips,
    LOGICAL_OR(tgt_is_new_district) AS is_new,
    MAX(tgt_n_src)                  AS n_src,
    MAX(tgt_n_places)               AS n_places,
    MAX(tgt_rural_src_share)        AS rural_share
  FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp`
  WHERE tgt_is_new_district IS NOT NULL
  GROUP BY 1, 2, 3
),
s AS (
  SELECT t.*, min_places,
    COALESCE(n_src >= 4 AND n_places >= min_places AND rural_share < 0.5, FALSE) AS is_wide
  FROM t, UNNEST([10, 15, 20, 25, 30]) AS min_places
)
SELECT
  county,
  min_places AS prog_min_places,
  COUNTIF(is_new OR is_wide)                 AS n_targets,
  SUM(IF(is_new OR is_wide, n_clips, 0))     AS n_clips_city_program,
  COUNTIF(is_wide AND NOT is_new)            AS n_targets_wide_only,
  SUM(IF(is_wide AND NOT is_new, n_clips, 0)) AS n_clips_wide_only
FROM s
GROUP BY 1, 2
ORDER BY 1, 2;
