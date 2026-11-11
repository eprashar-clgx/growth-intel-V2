-- Zoning change MVP2: R1-R4, R6-R10 copied from MVP1, then the growth signal (see zoning/docs/zoning_mvp2.md)
-- Independent of MVP1: doesn't read zoning_change_mvp1. Compare the two by joining outputs on clip (#33).
-- Output: one row per CLIP whose zoning district changed, with its change class and, for rezoning candidates,
-- the features, change pattern, growth evidence and growth confidence score
DECLARE fips_list        ARRAY<STRING> DEFAULT ['04013', '12001'];  -- Maricopa AZ, Alachua FL
DECLARE min_group_n      INT64   DEFAULT 20;    -- R8, R9
DECLARE regime_chg_share FLOAT64 DEFAULT 0.5;   -- R8
DECLARE rename_share     FLOAT64 DEFAULT 0.9;   -- R9
DECLARE qmi_m                   FLOAT64 DEFAULT 402.336;          -- features: quarter-mile neighborhood and patch link distance
DECLARE prev_snapshot_date      DATE    DEFAULT DATE '2025-09-03'; -- yb_status: PREV = gridics_zoning_archive_250903
DECLARE recent_yb_years         INT64   DEFAULT 2;    -- yb_status: RECENT if max_yr_built >= PREV year - 2 (to try later: current year - 2)
DECLARE isolated_max_cand_share FLOAT64 DEFAULT 0.05; -- change pattern: ISOLATED
DECLARE tract_min_same_tr_share FLOAT64 DEFAULT 0.5;  -- change pattern: TRACT vs MIXED
DECLARE owner_min_share         FLOAT64 DEFAULT 0.5;  -- growth evidence: owner
DECLARE land_min_share          FLOAT64 DEFAULT 0.75; -- growth evidence: land
DECLARE score_high_min          INT64   DEFAULT 3;    -- growth confidence: HIGH
DECLARE score_medium_min        INT64   DEFAULT 1;    -- growth confidence: MEDIUM

-- R2: strip punctuation/whitespace, uppercase
CREATE TEMP FUNCTION dcode(x STRING) AS (
  NULLIF(UPPER(REGEXP_REPLACE(x, r'[^A-Za-z0-9]', '')), '')
);
-- every district in set a is also in set b
CREATE TEMP FUNCTION is_sub(a STRING, b STRING) AS (
  NOT EXISTS (SELECT 1 FROM UNNEST(SPLIT(a, '|')) AS x WHERE x NOT IN UNNEST(SPLIT(b, '|')))
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

-- Lookups per changed CLIP: growth table flag, parcel, parcel essentials, super parcel, year-built status
CREATE TEMP TABLE lkp AS
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
  (g.clip IS NOT NULL) AS is_growth_clip,
  m.parcel_shape_id,
  pe.owner_name,
  pe.max_yr_built,
  (sp.sp_id IS NOT NULL) AS is_super_parcel,
  sp.sp_id,
  sp.sp_owner_name,
  -- UNMATCHED (no parcel or no parcel essentials row) is kept apart from NO_YEAR_BUILT (matched, year NULL)
  CASE
    WHEN pe.parcel_shape_id IS NULL                                                     THEN 'UNMATCHED'
    WHEN pe.max_yr_built IS NULL                                                        THEN 'NO_YEAR_BUILT'
    WHEN pe.max_yr_built >= EXTRACT(YEAR FROM prev_snapshot_date) - recent_yb_years     THEN 'RECENT'
    ELSE 'OLD'
  END AS yb_status
FROM chg AS c
LEFT JOIN growth AS g USING (clip)
LEFT JOIN c2p    AS m USING (clip)
LEFT JOIN pe          USING (parcel_shape_id)
LEFT JOIN sp          ON sp.parcel_shape_id = m.parcel_shape_id;

-- Features: one row per rezoning candidate (quarter-mile neighborhood + patch)
CREATE TEMP TABLE feat AS
WITH
cand AS (
  SELECT c.clip, c.fips, c.prev_dset, c.curr_dset, c.geog, l.owner_name, l.sp_id, l.yb_status
  FROM chg AS c
  JOIN lkp AS l USING (clip)
  WHERE c.stage1 = 'CANDIDATE'
),
-- quarter-mile neighborhood: sites are CLIPs in j, changed or not; co-located = within 1 m
nbr AS (
  SELECT
    c.clip,
    ST_DISTANCE(c.geog, s.geog) AS d,
    (x.clip IS NOT NULL) AS is_cand,
    (x.prev_dset = c.prev_dset AND x.curr_dset = c.curr_dset) AS same_tr
  FROM cand AS c
  JOIN j AS s ON ST_DWITHIN(c.geog, s.geog, qmi_m)
  LEFT JOIN chg AS x ON x.clip = s.clip AND x.stage1 = 'CANDIDATE'
  WHERE c.geog IS NOT NULL AND s.geog IS NOT NULL AND s.clip != c.clip
),
q AS (
  SELECT clip,
    COUNTIF(d <= 1)                       AS qmi_n_colocated,
    COUNTIF(d > 1)                        AS qmi_n_sites,
    COUNTIF(d > 1 AND is_cand)            AS qmi_n_cand,
    COUNTIF(d > 1 AND is_cand AND same_tr) AS qmi_n_same_tr
  FROM nbr
  GROUP BY clip
),
-- patch: candidates with coordinates and the same target district set, linked within 1/4 mile (co-located included)
pt AS (
  SELECT clip, owner_name, sp_id, yb_status,
    CONCAT(fips, ':', curr_dset, ':',
      CAST(ST_CLUSTERDBSCAN(geog, qmi_m, 1) OVER (PARTITION BY fips, curr_dset) AS STRING)) AS patch_id
  FROM cand
  WHERE geog IS NOT NULL
),
ps AS (
  SELECT patch_id,
    COUNT(*) AS patch_n,
    SAFE_DIVIDE(COUNTIF(yb_status IN ('NO_YEAR_BUILT', 'RECENT')),
                COUNTIF(yb_status != 'UNMATCHED')) AS patch_growth_yb_share
  FROM pt
  GROUP BY patch_id
),
-- most common patch owner / super parcel; NULL owner_name and NULL sp_id never count
po AS (
  SELECT patch_id, MAX(n) AS top_owner_n
  FROM (SELECT patch_id, owner_name, COUNT(*) AS n FROM pt WHERE owner_name IS NOT NULL GROUP BY 1, 2)
  GROUP BY patch_id
),
psp AS (
  SELECT patch_id, MAX(n) AS top_sp_n
  FROM (SELECT patch_id, sp_id, COUNT(*) AS n FROM pt WHERE sp_id IS NOT NULL GROUP BY 1, 2)
  GROUP BY patch_id
)
SELECT
  c.clip,
  c.yb_status,
  IF(c.geog IS NULL, NULL, IFNULL(q.qmi_n_sites, 0))     AS qmi_n_sites,
  IF(c.geog IS NULL, NULL, IFNULL(q.qmi_n_colocated, 0)) AS qmi_n_colocated,
  IF(c.geog IS NULL, NULL, IFNULL(q.qmi_n_cand, 0))      AS qmi_n_cand,
  IF(c.geog IS NULL, NULL, IFNULL(q.qmi_n_same_tr, 0))   AS qmi_n_same_tr,
  IF(c.geog IS NULL, NULL, IFNULL(SAFE_DIVIDE(q.qmi_n_cand, q.qmi_n_sites), 0)) AS qmi_cand_share,
  SAFE_DIVIDE(q.qmi_n_same_tr, q.qmi_n_cand)             AS qmi_same_tr_share,
  pt.patch_id,
  ps.patch_n,
  IF(pt.patch_id IS NULL, NULL, IFNULL(po.top_owner_n, 0) / ps.patch_n) AS patch_top_owner_share,
  IF(pt.patch_id IS NULL, NULL, IFNULL(psp.top_sp_n, 0) / ps.patch_n)   AS patch_top_sp_share,
  ps.patch_growth_yb_share
FROM cand AS c
LEFT JOIN q        USING (clip)
LEFT JOIN pt       USING (clip)
LEFT JOIN ps       ON ps.patch_id = pt.patch_id
LEFT JOIN po       ON po.patch_id = pt.patch_id
LEFT JOIN psp      ON psp.patch_id = pt.patch_id;

-- Final table: change pattern -> growth evidence -> change class -> growth confidence score
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2` AS
WITH
gate AS (
  SELECT
    c.clip,
    -- change pattern (NULL for non-candidates and for candidates without coordinates)
    CASE
      WHEN c.stage1 != 'CANDIDATE' OR c.geog IS NULL        THEN NULL
      WHEN f.qmi_cand_share <= isolated_max_cand_share      THEN 'ISOLATED'
      WHEN f.qmi_same_tr_share >= tract_min_same_tr_share   THEN 'TRACT'
      ELSE 'MIXED'
    END AS change_pattern,
    -- growth evidence: a one-CLIP patch has none
    IF(c.stage1 != 'CANDIDATE', NULL, COALESCE(f.patch_n >= 2
      AND (f.patch_top_owner_share >= owner_min_share OR f.patch_top_sp_share >= owner_min_share), FALSE)) AS has_owner_evidence,
    IF(c.stage1 != 'CANDIDATE', NULL, COALESCE(f.patch_n >= 2
      AND f.patch_growth_yb_share >= land_min_share, FALSE)) AS has_land_evidence
  FROM chg AS c
  LEFT JOIN feat AS f USING (clip)
),
-- growth confidence score: all weights live in this one block; each input -> (label, points)
pts AS (
  SELECT
    c.clip,
    IF(c.stage1 != 'CANDIDATE', NULL, ARRAY(
      SELECT AS STRUCT * FROM UNNEST([
        STRUCT(l.yb_status AS input,
          CASE l.yb_status WHEN 'NO_YEAR_BUILT' THEN 2 WHEN 'RECENT' THEN 2 WHEN 'OLD' THEN -1 ELSE 0 END AS points),
        STRUCT(g.change_pattern, IF(g.change_pattern = 'ISOLATED', 2, 0)),
        STRUCT('owner', IF(g.has_owner_evidence, 1, 0)),
        STRUCT('land', IF(g.has_land_evidence, 1, 0)),
        STRUCT('reg_move', IF(c.is_reg_move, 1, 0))
        -- is_partial_rezone and intensity_direction: 0 points (reported only)
      ]) WHERE points != 0
    )) AS p
  FROM chg AS c
  JOIN lkp  AS l USING (clip)
  JOIN gate AS g USING (clip)
),
score AS (
  SELECT
    clip,
    IF(p IS NULL, NULL, (SELECT IFNULL(SUM(points), 0) FROM UNNEST(p))) AS growth_score,
    IF(p IS NULL, NULL,
      IFNULL((SELECT STRING_AGG(FORMAT('%s %+d', input, points), '; ') FROM UNNEST(p)), '(none)')) AS growth_score_inputs
  FROM pts
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
  -- change class
  CASE
    WHEN c.stage1 != 'CANDIDATE'                            THEN c.stage1
    WHEN c.geog IS NULL                                     THEN 'NO_COORDS'
    WHEN g.change_pattern = 'ISOLATED'                      THEN 'NEEDLE_PARCEL'
    WHEN g.has_owner_evidence OR g.has_land_evidence        THEN 'DEVELOPER_TRACT'
    WHEN g.change_pattern = 'TRACT'                         THEN 'MASS_CHANGE'
    ELSE                                                         'CITY_PROGRAM'
  END AS zoning_change_class,
  s.growth_score,
  CASE
    WHEN s.growth_score IS NULL              THEN NULL
    WHEN s.growth_score >= score_high_min    THEN 'HIGH'
    WHEN s.growth_score >= score_medium_min  THEN 'MEDIUM'
    ELSE 'LOW'
  END AS growth_confidence,
  s.growth_score_inputs,
  g.change_pattern,
  g.has_owner_evidence,
  g.has_land_evidence,
  c.is_partial_rezone,
  c.is_reg_move,
  l.is_growth_clip,
  l.parcel_shape_id,
  l.owner_name,
  l.max_yr_built,
  l.yb_status,
  l.is_super_parcel,
  l.sp_id,
  l.sp_owner_name,
  c.reg_n, c.reg_chg_share,
  f.qmi_n_sites, f.qmi_n_colocated, f.qmi_n_cand, f.qmi_n_same_tr, f.qmi_cand_share, f.qmi_same_tr_share,
  f.patch_id, f.patch_n, f.patch_top_owner_share, f.patch_top_sp_share, f.patch_growth_yb_share,
  c.lat, c.lon,
  c.geog AS geom
FROM chg AS c
JOIN lkp       AS l USING (clip)
JOIN gate      AS g USING (clip)
JOIN score     AS s USING (clip)
LEFT JOIN feat AS f USING (clip);

-- S1: change class x growth table coverage
SELECT
  county,
  zoning_change_class,
  COUNT(*)                AS n_clips,
  COUNTIF(is_growth_clip) AS n_in_growth,
  COUNTIF(NOT is_growth_clip) AS n_not_in_growth,
  ROUND(100 * COUNTIF(is_growth_clip) / COUNT(*), 1) AS pct_in_growth
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2`
GROUP BY 1, 2
ORDER BY county, n_clips DESC;

-- S2: top transitions per change class (sanity check)
SELECT
  county,
  zoning_change_class,
  COUNT(*) AS n_clips,
  APPROX_TOP_COUNT(transition, 5) AS top_transitions
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2`
GROUP BY 1, 2
ORDER BY county, n_clips DESC;

-- S3: parcel essentials match rate + year-built status of rezoning candidates (growth_score IS NOT NULL)
SELECT
  county,
  COUNT(*)                                  AS n_clips,
  COUNT(DISTINCT clip)                      AS n_distinct_clips,
  ROUND(100 * COUNTIF(parcel_shape_id IS NOT NULL) / COUNT(*), 1) AS pct_with_parcel,
  ROUND(100 * COUNTIF(owner_name IS NOT NULL) / COUNT(*), 1)      AS pct_with_owner_name,
  ROUND(100 * COUNTIF(max_yr_built IS NOT NULL) / COUNT(*), 1)    AS pct_with_yr_built,
  COUNTIF(max_yr_built = 0)                 AS n_yr_built_zero,
  COUNTIF(growth_score IS NOT NULL)                                 AS n_candidates,
  COUNTIF(growth_score IS NOT NULL AND yb_status = 'UNMATCHED')     AS n_cand_unmatched,
  COUNTIF(growth_score IS NOT NULL AND yb_status = 'NO_YEAR_BUILT') AS n_cand_no_year_built,
  COUNTIF(growth_score IS NOT NULL AND yb_status = 'RECENT')        AS n_cand_recent,
  COUNTIF(growth_score IS NOT NULL AND yb_status = 'OLD')           AS n_cand_old
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2`
GROUP BY 1
ORDER BY 1;

-- S4: change class x super parcel x growth confidence
SELECT
  county,
  zoning_change_class,
  is_super_parcel,
  COUNT(*)                              AS n_clips,
  COUNT(DISTINCT sp_id)                 AS n_distinct_sp,
  COUNTIF(growth_confidence = 'HIGH')   AS n_high,
  COUNTIF(growth_confidence = 'MEDIUM') AS n_medium,
  COUNTIF(growth_confidence = 'LOW')    AS n_low
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2`
GROUP BY 1, 2, 3
ORDER BY county, zoning_change_class, is_super_parcel;

-- S5: feature summary per county x change class (rezoning candidates)
SELECT
  county,
  zoning_change_class,
  COUNT(*)                                                    AS n_clips,
  COUNT(DISTINCT patch_id)                                    AS n_patches,
  APPROX_QUANTILES(qmi_n_sites, 2)[OFFSET(1)]                 AS med_qmi_n_sites,
  APPROX_QUANTILES(qmi_n_cand, 2)[OFFSET(1)]                  AS med_qmi_n_cand,
  ROUND(APPROX_QUANTILES(qmi_cand_share, 2)[OFFSET(1)], 3)    AS med_qmi_cand_share,
  ROUND(APPROX_QUANTILES(qmi_same_tr_share, 2)[OFFSET(1)], 3) AS med_qmi_same_tr_share,
  APPROX_QUANTILES(patch_n, 2)[OFFSET(1)]                     AS med_patch_n,
  MAX(patch_n)                                                AS max_patch_n,
  ROUND(AVG(patch_top_owner_share), 2)                        AS avg_patch_top_owner_share,
  ROUND(AVG(patch_top_sp_share), 2)                           AS avg_patch_top_sp_share,
  ROUND(AVG(patch_growth_yb_share), 2)                        AS avg_patch_growth_yb_share,
  ROUND(100 * COUNTIF(has_owner_evidence) / COUNT(*), 1)      AS pct_owner_evidence,
  ROUND(100 * COUNTIF(has_land_evidence) / COUNT(*), 1)       AS pct_land_evidence,
  ROUND(100 * COUNTIF(is_reg_move) / COUNT(*), 1)             AS pct_reg_move,
  ROUND(AVG(growth_score), 2)                                 AS avg_growth_score,
  MIN(growth_score)                                           AS min_growth_score,
  MAX(growth_score)                                           AS max_growth_score
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2`
WHERE growth_score IS NOT NULL
GROUP BY 1, 2
ORDER BY county, n_clips DESC;

-- S6: hand check on the 72 labelled CLIPs (#26, all Maricopa AZ) vs zoning_mvp2.md "Hand check on the labels"
-- doc_expected_score is NULL for labels the doc doesn't name
WITH labels AS (
  SELECT * FROM UNNEST(ARRAY<STRUCT<clip STRING, label BOOL, note STRING>>[
    ('8512568282', TRUE, 'round1'),
    ('1022710468', TRUE, 'round1'),
    ('1015633102', TRUE, 'round1'),
    ('1098443201', TRUE, 'round1'),
    ('1019795280', TRUE, 'round1'),
    ('1025902746', TRUE, 'round1'),
    ('1029097217', TRUE, 'round1'),
    ('1021898474', TRUE, 'round1'),
    ('1023741108', TRUE, 'round1'),
    ('1028412838', TRUE, 'round1'),
    ('1024780462', TRUE, 'round1'),
    ('1026070683', TRUE, 'round1'),
    ('1018295170', TRUE, 'round1'),
    ('1026515894', TRUE, 'round1'),
    ('1019490831', TRUE, 'round1'),
    ('1025656645', TRUE, 'round1'),
    ('1028498385', TRUE, 'round1'),
    ('1025557408', TRUE, 'round1'),
    ('1027933203', TRUE, 'round1'),
    ('1031464503', TRUE, 'round1'),
    ('1030133639', TRUE, 'round1'),
    ('1017920444', TRUE, 'round1'),
    ('1020752080', TRUE, 'round1'),
    ('1028498953', TRUE, 'round1'),
    ('1021316614', TRUE, 'round1'),
    ('1051048463', TRUE, 'round2'),
    ('1196603140', TRUE, 'round2'),
    ('2948452474', TRUE, 'round2'),
    ('8497469251', TRUE, 'round2'),
    ('8562051457', TRUE, 'round2'),
    ('1018639374', TRUE, 'round2'),
    ('1027523579', TRUE, 'round2'),
    ('1029143780', TRUE, 'round2'),
    ('1023848567', TRUE, 'round2'),
    ('1027523567', TRUE, 'round2'),
    ('1024158090', TRUE, 'round2'),
    ('1031055754', TRUE, 'round2'),
    ('1021898682', TRUE, 'round2'),
    ('1018601749', TRUE, 'round2'),
    ('1030142028', TRUE, 'round2'),
    ('1029321738', TRUE, 'round2'),
    ('1018748204', TRUE, 'round2'),
    ('1026317469', TRUE, 'round2'),
    ('1022233039', TRUE, 'round2'),
    ('1026754682', TRUE, 'round2'),
    ('1028990247', TRUE, 'round2'),
    ('1019194973', TRUE, 'round2'),
    ('1020108566', TRUE, 'round2'),
    ('1028223601', TRUE, 'round2'),
    ('1021426212', TRUE, 'round2'),
    ('1017967553', TRUE, 'round2'),
    ('1031008879', TRUE, 'round2'),
    ('1018601833', TRUE, 'round2'),
    ('4061442078', TRUE, 'round2'),
    ('2195513116', TRUE, 'round2'),
    ('2266945902', TRUE, 'round2'),
    ('7701494706', TRUE, 'round2'),
    ('7302151055', TRUE, 'round2'),
    ('7309079749', TRUE, 'round2'),
    ('4612687086', TRUE, 'round2'),
    ('1102428042', TRUE, 'round2'),
    ('1085804251', TRUE, 'round2'),
    ('1100065945', TRUE, 'round2'),
    ('8407379818', TRUE, 'round2'),
    ('1094810868', TRUE, 'round2'),
    ('2315072472', TRUE, 'round2'),
    ('1497960503', TRUE, 'round2 vacant lot next to T4NF/T5MS false positives'),
    ('8657004225', TRUE, 'round2 vacant lot T4NF'),
    ('9105121030', FALSE, 'built parcel in Mesa transect remap'),
    ('8383032929', FALSE, 'built parcel in Mesa transect remap'),
    ('2322039587', FALSE, 'built parcel in Mesa transect remap'),
    ('2419864454', FALSE, 'built parcel in Mesa transect remap')
  ])
),
x AS (
  SELECT
    l.clip, l.label, l.note,
    t.transition, t.zoning_change_class, t.yb_status, t.max_yr_built, t.change_pattern,
    t.has_owner_evidence, t.has_land_evidence, t.is_reg_move,
    t.qmi_n_sites, t.qmi_n_cand, t.qmi_n_same_tr,
    ROUND(t.qmi_cand_share, 3) AS qmi_cand_share, ROUND(t.qmi_same_tr_share, 3) AS qmi_same_tr_share,
    t.patch_n, ROUND(t.patch_top_owner_share, 2) AS patch_top_owner_share,
    ROUND(t.patch_top_sp_share, 2) AS patch_top_sp_share, ROUND(t.patch_growth_yb_share, 2) AS patch_growth_yb_share,
    t.growth_score, t.growth_confidence, t.growth_score_inputs,
    CASE
      WHEN NOT l.label                  THEN 'Mesa false positive (OLD, MIXED)'
      WHEN t.transition = 'AG → RSL30'  THEN 'AG → RSL30 tract'
      WHEN t.transition = 'RS35 → PS'   THEN 'RS35 → PS needle'
      WHEN t.transition = 'LC → RSL30'  THEN 'LC → RSL30 in the RSL30 patch'
      WHEN t.transition = 'DC → T4NF'   THEN 'no year built lot in Mesa T4NF remap'
    END AS doc_case
  FROM labels AS l
  LEFT JOIN `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2` AS t USING (clip)
)
SELECT
  *,
  CASE doc_case
    WHEN 'Mesa false positive (OLD, MIXED)'     THEN -1
    WHEN 'AG → RSL30 tract'                     THEN 4
    WHEN 'RS35 → PS needle'                     THEN 4
    WHEN 'LC → RSL30 in the RSL30 patch'        THEN 4
    WHEN 'no year built lot in Mesa T4NF remap' THEN 2
  END AS doc_expected_score
FROM x
ORDER BY doc_case IS NULL, doc_case, transition, clip;

-- S6b: score per change class, change pattern and year-built status vs zoning_mvp2.md step 3
-- (needles are never LOW; an isolated rezoning on an OLD structure is expected at +1)
SELECT
  county,
  zoning_change_class,
  change_pattern,
  yb_status,
  growth_score,
  growth_confidence,
  COUNT(*) AS n_clips
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2`
WHERE growth_score IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY county, zoning_change_class, change_pattern, yb_status, growth_score;

-- S7: R1-R10 match with MVP1 (dev zoning_change_mvp1), joined on clip. stage1 is the change class of
-- non-candidates; every other class is a rezoning candidate. Expect n_only_mvp1 = n_only_mvp2 = n_stage1_diff = 0.
WITH
v1 AS (
  SELECT clip, county,
    IF(zoning_change_class IN ('CODE_REGIME_SWITCH', 'RENAME_OR_MERGE', 'SLIVER_DROP'), zoning_change_class, 'CANDIDATE') AS stage1
  FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp1`
),
v2 AS (
  SELECT clip, county,
    IF(zoning_change_class IN ('CODE_REGIME_SWITCH', 'RENAME_OR_MERGE', 'SLIVER_DROP'), zoning_change_class, 'CANDIDATE') AS stage1
  FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_mvp2`
)
SELECT
  COALESCE(v2.county, v1.county)  AS county,
  COALESCE(v2.stage1, v1.stage1)  AS stage1,
  COUNT(v1.clip)                  AS n_mvp1,
  COUNT(v2.clip)                  AS n_mvp2,
  COUNTIF(v2.clip IS NULL)        AS n_only_mvp1,
  COUNTIF(v1.clip IS NULL)        AS n_only_mvp2,
  COUNTIF(v1.stage1 != v2.stage1) AS n_stage1_diff
FROM v1
FULL JOIN v2 USING (clip)
GROUP BY 1, 2
ORDER BY 1, 2;
