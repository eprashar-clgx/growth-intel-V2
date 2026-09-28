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