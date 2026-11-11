# Glossary (domain model)

The shared vocabulary for this repo. Use these terms in chats, docs, SQL comments, issues and PRs.

**Rules**
- Use the **canonical term** exactly as written here. Don't coin synonyms.
- Terms in the **Avoid** column are known aliases. Replace them, and check for them with `python3 .github/skills/domain-glossary/lint_glossary.py`.
- The **Code** column gives the exact column, label or table name. Where SQL and prose disagree, the latest zoning SQL (currently MVP1) wins.
- Add or change terms on a branch through the issue workflow, using the `domain-glossary` skill. When renaming a term, move the old name into **Avoid**.

## Domain model

```mermaid
erDiagram
  SNAPSHOT ||--o{ ZONING_RECORD : contains
  CLIP ||--o{ ZONING_RECORD : "has 1+ rows (zid)"
  ZONING_RECORD }o--|| DISTRICT : "zoned as"
  DISTRICT }o--|| REGULATION : "defined in"
  REGULATION }o--|| JURISDICTION : "adopted by"
  CLIP ||--|{ DISTRICT_SET : "one per snapshot"
  CLIP ||--o| TRANSITION : "PREV set to CURR set"
  TRANSITION ||--|| CHANGE_CLASS : "R6-R13 (MVP1) or R6-R10 + growth signal (MVP2)"
  CLIP }o--o| PATCH : "same transition within quarter mile (MVP2)"
  CLIP ||--o| GROWTH_CONFIDENCE_SCORE : "MVP2"
  CHANGE_CLASS }o--|| OUTCOME_GROUP : "rolls up to"
  CLIP ||--o| GROWTH_RECORD : "puid = clip"
  CLIP }o--o| PARCEL : "vw_clip_to_parcel"
  PARCEL }o--o| SUPER_PARCEL : "vw_lkp_superparcels"
  GROWTH_RECORD }o--|| GROWTH_STAGE : "derived from indicators"
```

## Shared

| Term | Definition | Avoid | Code | Source |
|---|---|---|---|---|
| **CLIP** | Cotality's unique parcel identifier; the join key across all datasets. Write it in uppercase in prose. | `parcel ID`, `clip id` | `clip`; `puid` in growth tables | — |
| **Parcel** | The real-world piece of land a CLIP identifies. Say "parcel" for the land and "CLIP" for the identifier or row. A parcel can hold several CLIPs (condos); CLIP → parcel comes from `vw_clip_to_parcel`. | | `parcel_shape_id` | — |
| **FIPS** | 5-digit county code, zero-padded. | `county code` | `fips` = `LPAD(fips_code, 5, '0')` | R1 |
| **County** | Written as "<Name> <ST>", e.g. "Maricopa AZ". POC counties: Maricopa AZ (04013), Alachua FL (12001), Orange FL (12095). "Gainesville" is a city jurisdiction inside Alachua FL, not a county. | | `county` | zoning MVP1 |
| **Growth table** | The PanoramiQ growth indicators view, one row per CLIP. | `growth-table` | `vw_edr_panoramiq_growth_indicators`, `…_v2`; `is_growth_clip` | growth_intel, zoning MVP1 |
| **Growth Intelligence V1 / V2** | V1 = production indicator logic in the growth table; V2 = the revised logic proposed in `growth_intel/`. Uppercase **V**. | | suffix `_V2` | `growth_intel/` |
| **Zoning POC version** | Iterations of the zoning work: v1, v2, v4, v6, v6b, then **MVP1** and **MVP2**. Lowercase **v** for POC iterations, so they don't collide with Growth Intelligence V1/V2. **MVP1** = rules R1–R4, R6–R13, frozen (bug fixes only). **MVP2** (#26) = R1–R10 plus growth-signal gates and a confidence score, built as a separate script. MVP1 and MVP2 are compared by joining their outputs on `clip`. MVP1/MVP2 are zoning versions, not Growth Intelligence V1/V2. | `MVP` | rule `Tag` column; `zoning_change_mvp1`, `zoning_change_mvp2` | `zoning_POC.md` |

## Zoning

### Data and entities

| Term | Definition | Avoid | Code | Source |
|---|---|---|---|---|
| **Gridics** | The zoning data vendor. | | `gridics_zoning*` | — |
| **Snapshot** | A dated copy of the Gridics zoning table. **PREV** = archive `gridics_zoning_archive_250903`; **CURR** = active `gridics_zoning`. | `old table`, `new table` | `prev_*`, `curr_*`, `prev_s`, `curr_s` | R1 |
| **Zoning record** | One row in a snapshot: a CLIP inside one vendor zone polygon. | | | R1 |
| **zid** | The vendor's zone polygon ID. It is renumbered on every reload, so never compare it across snapshots. | | `zid` | R3 |
| **District** | A zoning category code such as `R1` or `PD`. Always compared after normalization. | | `zoning_district`; normalized by `dcode()` | R2 |
| **District set** | A CLIP's sorted, de-duplicated, pipe-joined normalized districts in one snapshot, e.g. `C2\|R1`. | `zoning string` | `prev_dset`, `curr_dset` | R3 |
| **Split-zoned** | A CLIP whose district set contains more than one district. | `multi-district`, `multi-zoned`, `MULTI_DISTRICT` | `STRPOS(dset, '\|') > 0` | R3, R10 |
| **Regulation** | The named zoning code that defines districts, e.g. "Orange Code 2050". | `zoning code` | `zoning_regulation_name`; `prev_reg`, `curr_reg` | R8 |
| **Jurisdiction** | The county or city government that adopts a regulation. SQL approximates it as `(fips, regulation)`. | | | R8 |
| **Presence** | Whether a CLIP appears in both snapshots or only one. | | `presence` = `BOTH` / `ADDED` / `REMOVED` | R4 |
| **Coverage churn** | A CLIP in only one snapshot with no lineage parent. Counts as vendor coverage noise. | | | R4, R5 |
| **Parcel lineage** | The link from a child CLIP (created by a split) back to its parent CLIP. | | `lineage_parent_clip`, `is_split` *(planned)* | R5 |
| **Parcel essentials** | Cotality's parcel-level attribute table; the source of owner name and year built in zoning MVP1. Mostly one row per parcel, so MVP1 dedupes it on `parcel_shape_id`. | | `parcel_essentials_dsf` | zoning MVP1 |
| **Owner name** | The parcel's primary owner name from parcel essentials. Condo CLIPs on one parcel share it. | | `owner_name` (from `ownr1fullname`) | zoning MVP1 |
| **Super parcel** | An assemblage of parcels with the same owner, all within 75 m of each other, found by DBSCAN clustering in Cotality's property graph. Marketing documentation calls it **Parcel Link**; BigQuery tables use "super parcels". A parcel belongs to at most one super parcel. Not the same as several CLIPs on one parcel (`n_clips_on_parcel`). | | `is_super_parcel`, `sp_id`; `vw_lkp_superparcels` | zoning MVP1 |
| **Super parcel owner name** | The super parcel's owner from `vw_superparcels`. Kept separate from **Owner name** (parcel essentials). | | `sp_owner_name` (from `owner`) | zoning MVP1 |

### Pipeline concepts

| Term | Definition | Avoid | Code | Source |
|---|---|---|---|---|
| **Transition** | The pair PREV district set → CURR district set for a CLIP. | `change pair` | `transition`, `same_tr` | R7, R12 |
| **District change** | A CLIP whose district set differs between snapshots. This is the gate into classification. | | `prev_dset IS DISTINCT FROM curr_dset` | R7 |
| **Rule-value change** | A change to height, FAR, density, setbacks etc. with no district change. Out of scope for this indicator. | | v1 labels `RULES_CHANGED` etc. | R7 |
| **Zoning assigned / dropped** | The CLIP is in both snapshots, but its district set is empty on one side. | | `ZONING_ASSIGNED`, `ZONING_DROPPED` | R6 |
| **Code regime switch** | A jurisdiction's whole regulation was replaced, so its district changes can't be measured this period. | `regime change` | `CODE_REGIME_SWITCH`; `reg_n`, `reg_chg_share` | R8 |
| **Rename or merge** | A jurisdiction renamed or merged districts (e.g. `RSF1`–`RSF4` → `SF`). It is learned as a crosswalk. | `DISTRICT_RENAME_OR_MERGE` | `RENAME_OR_MERGE`; `rename_map`, `prev_dset_mapped` | R9 |
| **Rezoning candidate** | A district change that survived the noise filters R8–R10. | `REZONE_CANDIDATE`, `rezone candidate` | `stage1 = 'CANDIDATE'` | R8–R10 |
| **Sliver** | A split-zoned CLIP lost a district at a boundary (CURR set ⊂ PREV set). Counts as noise. | | `SLIVER_DROP` | R10 |
| **Partial rezoning** | A split-zoned CLIP gained a district (PREV set ⊂ CURR set). It continues through the pipeline. | | `is_partial_rezone` | R10 |
| **City program** | A city-led remap that looks like scattered rezonings. MVP1 (R11): a target district that is new or handed out widely. MVP2: a `MIXED` change pattern without growth evidence. | | `CITY_PROGRAM`; MVP1: `is_city_program`, `tgt_is_new_district`, `tgt_n_src` | R11 (MVP1), change class (MVP2) |
| **Places** | Distinct 300 m DBSCAN clusters that a target district was handed out in. | `tgt_n_clusters` | `tgt_n_places` | R11 (MVP1) |
| **Rural source share** | The share of a target district's rezonings that came from AG/rural districts. | `tgt_ag_src_share` | `tgt_rural_src_share` | R11 (MVP1) |
| **kNN isolation** | The share of a candidate's k nearest sites that made the same transition. Co-located sites (condos) are excluded. | `k20_same_tr` | `k_n`, `k_same_tr`, `k_same_tr_share`, `n_colocated` | R12 (MVP1) |
| **Tract size** | The number of same-transition CLIPs within the search radius. | `n_same_tr_1km` | `n_same_tr_radius` | R13 (MVP1) |
| **Regulation move** | Observed: a CLIP's regulation differs between PREV and CURR. | | `is_reg_move` | R14 |
| **Annexation** | Confirmed subset of regulation moves: county regulation → city regulation. Not yet implemented. | | `is_annexation` *(reserved)* | R14 |
| **Use class** | A broad keyword-based use category for a district (CONS, AG, SF, MF, MU, COM, IND). | | `prev_use_class`, `curr_use_class` | R15 |
| **Intensity direction** | Upzone / downzone / use change / unknown, measured from intensity values or inferred from use rank. | `up-zone`, `down-zone` | `intensity_direction` = `UPZONE` / `DOWNZONE` / `USE_CHANGE` / `UNKNOWN` | R15 |
| **Rezoning** | A real change to a parcel's district made by its jurisdiction. Use "rezone" only as a verb. | | | — |

### Growth signal (MVP2)

The steps that replace R11–R13 in MVP2 (`zoning_POC.md`, "MVP2: growth signal"). They have no R-number and are named by what they compute.

| Term | Definition | Avoid | Code | Source |
|---|---|---|---|---|
| **Site** | A CLIP in both snapshots with zoning on both sides (temp table `j`), changed or not. | | `qmi_n_sites` | R12, MVP2 features |
| **Co-located** | A CLIP within 1 m of another, usually a condo on the same parcel. It is excluded from neighbor counts and shares. | | `n_colocated` (MVP1), `qmi_n_colocated` (MVP2) | R12, MVP2 features |
| **Quarter-mile neighborhood** | All sites within 402.336 m (¼ mile) of a rezoning candidate, excluding co-located CLIPs. | | `qmi_m`; prefix `qmi_` | MVP2 features |
| **Candidate share** | The share of sites in the quarter-mile neighborhood that are rezoning candidates (any transition). | | `qmi_cand_share` = `qmi_n_cand / qmi_n_sites` | MVP2 features |
| **Same-transition share** | The share of rezoning candidates in the quarter-mile neighborhood with the candidate's transition. Not the same as MVP1's kNN isolation, whose denominator is all sites. | | `qmi_same_tr_share` = `qmi_n_same_tr / qmi_n_cand` | MVP2 features |
| **Patch** | Rezoning candidates with the same transition that are linked within ¼ mile (DBSCAN, `minPts` 1). | | `patch_id`, `patch_n` | MVP2 features |
| **Owner as of PREV** | The parcel's owner name at the PREV snapshot date, from parcel essentials history. *(planned, #29)* | | `owner_name_prev` | MVP2 features |
| **Patch owner** | The owner used for patch ownership: owner as of PREV, falling back to owner name. | | `COALESCE(owner_name_prev, owner_name)` | MVP2 features |
| **Top-owner share** | The share of a patch's CLIPs held by its most common patch owner. | | `patch_top_owner_share` | MVP2 features |
| **Top super parcel share** | The share of a patch's CLIPs in its most common super parcel. | | `patch_top_sp_share` | MVP2 features |
| **Year-built status** | A CLIP's year built relative to PREV: `NO_YEAR_BUILT` (matched, `max_yr_built` NULL), `RECENT` (≥ PREV year − `recent_yb_years`), `OLD`, or `UNMATCHED` (no parcel essentials row). `NO_YEAR_BUILT` is not the same as **Vacant**. | | `yb_status` | MVP2 features |
| **Growth year-built share** | The share of a patch's matched CLIPs with `yb_status` `NO_YEAR_BUILT` or `RECENT`. | | `patch_growth_yb_share` | MVP2 features |
| **Change pattern** | The shape of the change around a rezoning candidate: `ISOLATED` (low candidate share), `TRACT` (high same-transition share) or `MIXED`. | | `change_pattern` | MVP2 |
| **Owner evidence** | A patch of 2+ CLIPs where one patch owner or one super parcel holds at least `owner_min_share`. | | `has_owner_evidence` | MVP2 growth evidence |
| **Land evidence** | A patch of 2+ CLIPs whose growth year-built share is at least `land_min_share`. | | `has_land_evidence` | MVP2 growth evidence |
| **Growth evidence** | Owner evidence or land evidence. It decides developer tract vs government change for `TRACT` and `MIXED` patterns. | | `has_owner_evidence OR has_land_evidence` | MVP2 |
| **Growth confidence score** | Additive points per rezoning candidate from year-built status, change pattern, growth evidence and regulation move. They are reported as a tier and an audit string. Not related to the growth table or growth stage. | | `growth_score`; `growth_confidence` = `HIGH` / `MEDIUM` / `LOW`; `growth_score_inputs` | MVP2 |

### Change classes and outcome groups

| Term | Definition | Avoid | Code | Source |
|---|---|---|---|---|
| **Change class** | The final per-CLIP label from the pipeline. | `bucket`, `buckets`, `change_class` | `zoning_change_class` | zoning MVP1 |
| **Outcome group** | One of three roll-ups of change classes: **noise**, **government change**, **development signal**. | `cosmetic change`, `jurisdiction-wide`, `district-wide`, `area rezoning`, `code-wide change` | | `zoning_POC.md` §1 |
| **Noise** | Outcome group: vendor or data artifacts. | | | §1 |
| **Government change** | Outcome group: changes made by a government or applied code-wide. | | | §1 |
| **Development signal** | Outcome group: rezonings a developer probably pursued; the product target. MVP1: `NEEDLE_PARCEL` + `SMALL_TRACT`. MVP2: `NEEDLE_PARCEL` + `DEVELOPER_TRACT`. | | | §1 |
| **Needle** | Only an isolated single-parcel rezoning (`NEEDLE_PARCEL`). Not a synonym for development signal. MVP1: kNN isolation; MVP2: `ISOLATED` change pattern. | | `NEEDLE_PARCEL` | R12 (MVP1), change pattern (MVP2) |
| **Small tract** | MVP1: a developer-scale group of same-transition CLIPs (≤ `tract_max_clips`). MVP2's counterpart is **Developer tract**. | | `SMALL_TRACT` | R13 (MVP1) |
| **Developer tract** | MVP2: a `TRACT` or `MIXED` change pattern with growth evidence. There is no size cap. | | `DEVELOPER_TRACT` | MVP2 |
| **Mass change** | A same-transition change that isn't a developer tract. MVP1: more than `tract_max_clips` CLIPs. MVP2: a `TRACT` change pattern without growth evidence. | | `MASS_CHANGE` | R13 (MVP1), MVP2 |

Change class → outcome group:

| `zoning_change_class` | Rule | Outcome group |
|---|---|---|
| `SLIVER_DROP` | R10 | Noise |
| `NO_COORDS` | — | Not grouped (no geometry). MVP2 still gives it a growth confidence score from CLIP-level inputs |
| `CODE_REGIME_SWITCH` | R8 | Government change |
| `RENAME_OR_MERGE` | R9 | Government change |
| `CITY_PROGRAM` | R11 (MVP1); change pattern + growth evidence (MVP2) | Government change |
| `MASS_CHANGE` | R13 (MVP1); change pattern + growth evidence (MVP2) | Government change |
| `NEEDLE_PARCEL` | R12 (MVP1); change pattern (MVP2) | Development signal |
| `SMALL_TRACT` | R13 (MVP1 only) | Development signal |
| `DEVELOPER_TRACT` | change pattern + growth evidence (MVP2 only) | Development signal |
| `ZONING_ASSIGNED` / `ZONING_DROPPED` | R6 | Open item (v2 only, not grouped) |

## Growth Intel

| Term | Definition | Avoid | Code | Source |
|---|---|---|---|---|
| **Base table** | The per-CLIP analysis table built by `base_table.sql`, which the V2 indicators read. | | `growth_intelligence_base_qgis` | `base_table.sql` |
| **Growth stage** | The per-CLIP stage derived from indicators: **Early Growth**, **Ongoing Growth**, **Recently Completed**. V1 also had a **New Clip** stage; V2 folds new CLIPs into Early Growth. | | `growth_stage`, `growth_stage_V2` | `V2_indicators.sql` |
| **Indicator** | A Y/N flag per CLIP that feeds the growth stage. | | `*_indicator`, `*_indicator_V2` | — |
| **Land use change indicator** | The land use code changed. Feeds Early Growth. | | `landuse_change_indicator`, `land_use_change_indicator_V2` | README |
| **Builder/developer ownership indicator** | The CLIP is owned by a builder or developer. Feeds Early Growth. | `builder developer` | `builder_developer_ownership_indicator`, `…_V2` | README |
| **New CLIP indicator** | The CLIP was newly created. Feeds Early Growth in V2. | | `new_clip_indicator`, `new_clip_indicator_V2` | README |
| **New construction permit indicator** | A recent new-construction building permit exists. Feeds Ongoing Growth. | | `recent_new_con_bldg_permit_indicator`, `…_V2` | README |
| **Recent year built indicator** | Year built is recent. Feeds Recently Completed. | | `recent_yearbuilt_indicator` | `base_table.sql` |
| **Recent new construction sale indicator** | A recent sale of new construction exists. Feeds Recently Completed. | | `recent_new_construction_sale_indicator` | `base_table.sql` |
| **Year built** | The latest actual year built, falling back to the latest effective year built. | | `max_yr_built` = `COALESCE(latest_year_built, latest_eff_year_built)` (from `yybltactdt`, `yyblteffdt`) | `base_table.sql`, zoning MVP1 (from parcel essentials) |
| **Vacant** | The CLIP has vacant delivery points. **Occupied** = not vacant (ratio 0 or NULL). | | `addr_vacant_ratio > 0` | `base_table.sql` |
| **Latest sale date** | The most recent recorded transaction for the CLIP. | | `max_transaction_date` (from `recordingdt`) | `V2_indicators.sql` |
| **Number of structures** | Count of buildings on the CLIP. | | `num_structures` *(proposed)* | README |
