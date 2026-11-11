# Zoning change indicator (POC)

Finds parcel rezonings a developer probably pursued, separating them from vendor noise and government/code-wide changes. Design, product rules and decision flow: [`docs/zoning_mvp2.md`](docs/zoning_mvp2.md) (MVP2, active). MVP1 rule catalogue (frozen): [`docs/zoning_POC_mvp1.md`](docs/zoning_POC_mvp1.md).

## SQL

| File | Purpose |
|---|---|
| [`sql/zoning_change_mvp1.sql`](sql/zoning_change_mvp1.sql) | MVP1 (frozen): rules R1–R4, R6–R13; one row per changed CLIP with its change class, growth table flag, and parcel essentials attributes (`parcel_shape_id`, `owner_name`, `max_yr_built`) for QA |
| [`sql/zoning_change_mvp1_parcels.sql`](sql/zoning_change_mvp1_parcels.sql) | Joins MVP1 output to parcel polygons for CARTO (carries `owner_name`, `max_yr_built`). Run after `zoning_change_mvp1.sql` |
| [`sql/scratch/`](sql/scratch/) | Exploratory queries; not maintained |
