# Zoning change indicator (POC)

Finds parcel rezonings a developer probably pursued, separating them from vendor noise and government/code-wide changes. Full rule catalogue and decision flow: [`docs/zoning_POC.md`](docs/zoning_POC.md).

## SQL

| File | Purpose |
|---|---|
| [`sql/zoning_poc_v1.sql`](sql/zoning_poc_v1.sql) | v1: snapshot prep, CLIP match and change flags for sample counties |
| [`sql/zoning_poc_v2.sql`](sql/zoning_poc_v2.sql) | v2: separates jurisdiction/district-wide and cosmetic change from parcel and area rezoning |
| [`sql/zoning_change_mvp.sql`](sql/zoning_change_mvp.sql) | MVP: rules R1–R4, R6–R13; one row per changed CLIP with its change class, growth table flag, and parcel essentials attributes (`parcel_shape_id`, `owner_name`, `max_yr_built`) for QA |
| [`sql/zoning_change_mvp_parcels.sql`](sql/zoning_change_mvp_parcels.sql) | Joins MVP output to parcel polygons for CARTO (carries `owner_name`, `max_yr_built`). Run after `zoning_change_mvp.sql` |
| [`sql/scratch/`](sql/scratch/) | Exploratory queries; not maintained |
