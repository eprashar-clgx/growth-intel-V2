-- Zoning change POC v2: separate jurisdiction-wide / district-wide / cosmetic change from parcel & area rezoning
DECLARE fips_list ARRAY<STRING> DEFAULT ['12095', '12001', '04013'];
DECLARE reg_event_share      FLOAT64 DEFAULT 0.5;
DECLARE district_event_share FLOAT64 DEFAULT 0.8;
DECLARE rename_share         FLOAT64 DEFAULT 0.9;
DECLARE min_group_size       INT64   DEFAULT 20;

CREATE TEMP FUNCTION dcode(x STRING) AS (NULLIF(REGEXP_REPLACE(UPPER(x), r'[^A-Z0-9]', ''), ''));
CREATE TEMP FUNCTION set_norm(s STRING) AS ((
  SELECT STRING_AGG(DISTINCT d, '|' ORDER BY d)
  FROM (SELECT dcode(x) AS d FROM UNNEST(SPLIT(s, ' | ')) AS x)
  WHERE d IS NOT NULL
));
CREATE TEMP FUNCTION is_subset(a STRING, b STRING) AS ((
  SELECT LOGICAL_AND(d IN UNNEST(SPLIT(b, '|'))) FROM UNNEST(SPLIT(a, '|')) AS d
));
-- Heuristic; check the OTHER share in S4 and tune
CREATE TEMP FUNCTION use_class(code STRING, descr STRING) AS (
  CASE
    WHEN dcode(code) IS NULL THEN 'UNZONED'
    WHEN REGEXP_CONTAINS(UPPER(IFNULL(descr, '')), r'PLANNED')
      OR REGEXP_CONTAINS(dcode(code), r'^(PD|PUD|PAD|PC)') THEN 'PLANNED_DEV'
    WHEN REGEXP_CONTAINS(UPPER(IFNULL(descr, '')), r'AGRIC|RURAL|RANCH|FARM')
      OR REGEXP_CONTAINS(dcode(code), r'^(AG|A[0-9]|RU|T[12])') OR dcode(code) = 'A' THEN 'AGRICULTURAL'
    WHEN REGEXP_CONTAINS(UPPER(IFNULL(descr, '')), r'INDUSTR|MANUFACT|WAREHOUS')
      OR REGEXP_CONTAINS(dcode(code), r'^(IND|LI|HI|IP|I[0-9]|M[0-9])') THEN 'INDUSTRIAL'
    -- CHANGED: conservation matched on district code only at this point
    WHEN REGEXP_CONTAINS(dcode(code), r'^(CON|OS)$|^CONS') THEN 'CONSERVATION_OPEN'
    WHEN REGEXP_CONTAINS(UPPER(IFNULL(descr, '')), r'MIXED|DOWNTOWN|MAIN ?STREET|TRANSECT')
      OR REGEXP_CONTAINS(dcode(code), r'^(MU|MX|DC|DB|T[4-6])') THEN 'MIXED_USE'
    WHEN REGEXP_CONTAINS(UPPER(IFNULL(descr, '')), r'COMMERC|BUSINESS|OFFICE|RETAIL')
      OR REGEXP_CONTAINS(dcode(code), r'^(C[0-9]|GC|NC|CC|CO|BUS|B[0-9]|O[0-9])') THEN 'COMMERCIAL'
    WHEN REGEXP_CONTAINS(UPPER(IFNULL(descr, '')), r'MULTI|APARTMENT|TOWN ?HOME|TOWNHOUSE|DUPLEX|TWO[- ]FAMILY|MEDIUM DENSITY|HIGH DENSITY')
      OR REGEXP_CONTAINS(dcode(code), r'^(RM|MF|RMF|R[2-6])') THEN 'MULTI_FAMILY'
    WHEN REGEXP_CONTAINS(UPPER(IFNULL(descr, '')), r'SINGLE|ONE[- ]FAMILY|ESTATE|LOW DENSITY|RESIDENTIAL')
      OR REGEXP_CONTAINS(dcode(code), r'^(RS|SF|R1|RE|RCE|RSL|T3|R[0-9])') THEN 'SINGLE_FAMILY'
    -- CHANGED: description-based conservation check moved to the end, as a fallback
    WHEN REGEXP_CONTAINS(UPPER(IFNULL(descr, '')), r'CONSERV|OPEN SPACE|PRESERV|RECREAT') THEN 'CONSERVATION_OPEN'
    ELSE 'OTHER'
  END
);

CREATE TEMP TABLE base AS
SELECT
  *,
  IFNULL(prev_regulation, '(none)') AS prev_reg,
  IFNULL(curr_regulation, '(none)') AS curr_reg,
  set_norm(prev_district_set) AS prev_dset,
  set_norm(curr_district_set) AS curr_dset,
  (far_chg OR height_chg OR density_chg OR lot_coverage_chg OR open_space_chg OR setback_chg) AS rule_chg,
  use_class(prev_district, prev_desc) AS prev_use_class,
  use_class(curr_district, curr_desc) AS curr_use_class
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v1_sample`
WHERE presence = 'BOTH';

CREATE TEMP TABLE classified AS
WITH
reg_stats AS (
  SELECT fips, prev_reg, curr_reg,
    COUNT(*) AS reg_n,
    SAFE_DIVIDE(COUNTIF(prev_dset IS DISTINCT FROM curr_dset), COUNT(*)) AS reg_district_chg_share,
    SAFE_DIVIDE(COUNTIF(rule_chg), COUNT(*)) AS reg_rule_chg_share
  FROM base
  GROUP BY 1, 2, 3
),
dist_stats AS (
  SELECT fips, prev_reg, prev_dset,
    COUNTIF(prev_dset = curr_dset) AS dist_n,
    SAFE_DIVIDE(COUNTIF(rule_chg AND prev_dset = curr_dset), COUNTIF(prev_dset = curr_dset)) AS dist_rule_chg_share
  FROM base
  WHERE prev_dset IS NOT NULL
  GROUP BY 1, 2, 3
),
xwalk AS (
  SELECT fips, prev_reg, prev_dset, curr_dset AS xwalk_dset, share AS xwalk_share
  FROM (
    SELECT fips, prev_reg, prev_dset, curr_dset,
      COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY fips, prev_reg, prev_dset) AS share,
      SUM(COUNT(*)) OVER (PARTITION BY fips, prev_reg, prev_dset) AS n_total,
      ROW_NUMBER() OVER (PARTITION BY fips, prev_reg, prev_dset ORDER BY COUNT(*) DESC) AS rk
    FROM base
    WHERE prev_dset IS NOT NULL AND curr_dset IS NOT NULL
    GROUP BY 1, 2, 3, 4
  )
  WHERE rk = 1 AND n_total >= min_group_size AND curr_dset != prev_dset
),
curr_codes AS (
  SELECT DISTINCT fips, curr_reg, code
  FROM base, UNNEST(SPLIT(curr_dset, '|')) AS code
),
retired AS (
  SELECT b.clip, LOGICAL_AND(cc.code IS NULL) AS prev_district_retired
  FROM base AS b
  CROSS JOIN UNNEST(SPLIT(b.prev_dset, '|')) AS d
  LEFT JOIN curr_codes AS cc
    ON cc.fips = b.fips AND cc.curr_reg = b.curr_reg AND cc.code = d
  WHERE b.prev_dset IS DISTINCT FROM b.curr_dset
  GROUP BY b.clip
)
SELECT
  b.*,
  r.reg_n, r.reg_district_chg_share, r.reg_rule_chg_share,
  ds.dist_n, ds.dist_rule_chg_share,
  x.xwalk_dset, x.xwalk_share,
  rt.prev_district_retired,
  CASE
    WHEN b.prev_dset IS NULL AND b.curr_dset IS NOT NULL THEN 'ZONING_ASSIGNED'
    WHEN b.prev_dset IS NOT NULL AND b.curr_dset IS NULL THEN 'ZONING_DROPPED'
    WHEN b.prev_reg != b.curr_reg AND r.reg_n >= min_group_size
         AND r.reg_district_chg_share >= reg_event_share              THEN 'CODE_REGIME_SWITCH'
    WHEN b.prev_dset IS DISTINCT FROM b.curr_dset
         AND x.xwalk_dset = b.curr_dset AND x.xwalk_share >= rename_share THEN 'DISTRICT_RENAME_OR_MERGE'
    WHEN b.prev_dset IS DISTINCT FROM b.curr_dset
         AND (is_subset(b.prev_dset, b.curr_dset) OR is_subset(b.curr_dset, b.prev_dset)) THEN 'SPLIT_BOUNDARY_ADJ'
    WHEN b.prev_dset IS DISTINCT FROM b.curr_dset                    THEN 'REZONE_CANDIDATE'
    WHEN b.rule_chg AND r.reg_n >= min_group_size
         AND r.reg_rule_chg_share >= reg_event_share                 THEN 'JURISDICTION_RULE_UPDATE'
    WHEN b.rule_chg AND ds.dist_n >= min_group_size
         AND ds.dist_rule_chg_share >= district_event_share          THEN 'DISTRICT_RULE_UPDATE'
    WHEN b.rule_chg                                                  THEN 'PARCEL_RULE_CHANGE'
    WHEN b.raw_district_chg                                          THEN 'COSMETIC_CODE'
    WHEN b.lot_area_chg OR b.capacity_chg                            THEN 'LOT_GEOMETRY_ONLY'
    ELSE 'NO_MATERIAL_CHANGE'
  END AS change_class
FROM base AS b
LEFT JOIN reg_stats  AS r  ON r.fips = b.fips AND r.prev_reg = b.prev_reg AND r.curr_reg = b.curr_reg
LEFT JOIN dist_stats AS ds ON ds.fips = b.fips AND ds.prev_reg = b.prev_reg AND ds.prev_dset = b.prev_dset
LEFT JOIN xwalk      AS x  ON x.fips = b.fips AND x.prev_reg = b.prev_reg AND x.prev_dset = b.prev_dset
LEFT JOIN retired    AS rt ON rt.clip = b.clip;

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v2_sample` AS
WITH
coords AS (
  SELECT clip, SAFE.ST_GEOGPOINT(clip_longitude, clip_latitude) AS geog
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_zoning.gridics_zoning`
  WHERE LPAD(fips_code, 5, '0') IN UNNEST(fips_list)
    AND clip_latitude IS NOT NULL AND clip_longitude IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY clip ORDER BY clgx_pmd_load_timestamp DESC) = 1
),
clustered AS (   -- same transition, >=5 CLIPs within 150 m => AREA; otherwise PARCEL
  SELECT c.clip, c.fips, c.prev_dset, c.curr_dset,
    ST_CLUSTERDBSCAN(k.geog, 150, 5) OVER (PARTITION BY c.fips, c.prev_dset, c.curr_dset) AS cluster_id
  FROM classified AS c
  JOIN coords AS k ON k.clip = c.clip
  WHERE c.change_class = 'REZONE_CANDIDATE' AND k.geog IS NOT NULL
),
sized AS (
  SELECT clip, cluster_id,
    IF(cluster_id IS NULL, 1, COUNT(*) OVER (PARTITION BY fips, prev_dset, curr_dset, cluster_id)) AS cluster_n
  FROM clustered
)
SELECT
  c.*,
  CASE
    WHEN c.change_class != 'REZONE_CANDIDATE' THEN NULL
    WHEN s.clip IS NULL THEN 'NO_COORDS'
    WHEN s.cluster_id IS NULL THEN 'PARCEL'
    ELSE 'AREA'
  END AS rezone_scale,
  s.cluster_id,
  s.cluster_n,
  CONCAT(c.prev_use_class, ' -> ', c.curr_use_class) AS use_transition
FROM classified AS c
LEFT JOIN sized AS s ON s.clip = c.clip;

-- S1: how much, by class
SELECT fips, change_class, rezone_scale,
  COUNT(*) AS n_clips,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY fips), 3) AS pct_of_county
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v2_sample`
GROUP BY 1, 2, 3
ORDER BY fips, n_clips DESC;

-- S2: what kind of rezoning
SELECT fips, rezone_scale, use_transition, intensity_direction,
  COUNT(*) AS n_clips,
  COUNT(DISTINCT CONCAT(prev_dset, '>', curr_dset)) AS n_code_pairs,
  COUNTIF(prev_district_retired) AS n_prev_district_retired
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v2_sample`
WHERE change_class = 'REZONE_CANDIDATE'
GROUP BY 1, 2, 3, 4
ORDER BY fips, n_clips DESC;

-- S3: sanity check of the bulk events that were detected
SELECT fips, change_class, prev_reg, curr_reg, prev_dset, curr_dset, COUNT(*) AS n_clips
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v2_sample`
WHERE change_class IN ('CODE_REGIME_SWITCH', 'DISTRICT_RENAME_OR_MERGE', 'JURISDICTION_RULE_UPDATE', 'DISTRICT_RULE_UPDATE')
GROUP BY 1, 2, 3, 4, 5, 6
QUALIFY ROW_NUMBER() OVER (PARTITION BY fips, change_class ORDER BY COUNT(*) DESC) <= 10
ORDER BY fips, change_class, n_clips DESC;

-- S4: how well the land-use classifier covers the codes
SELECT fips, curr_use_class, COUNT(*) AS n_clips,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY fips), 2) AS pct
FROM `clgx-gis-app-dev-06e3.work_eprashar.zoning_change_poc_v2_sample`
GROUP BY 1, 2
ORDER BY fips, n_clips DESC;