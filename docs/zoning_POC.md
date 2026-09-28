## 1. Decision flow

**Legend**
- 🟧 **Orange**: open item (a new column or data source, or a threshold that still needs tuning)
- ⬜ **Grey**: vendor or data noise
- 🟦 **Blue**: government or code-wide change
- 🟩 **Green**: development signal

Tags such as `[v1]` … `[v6b]` show the query version that introduced each rule. `[NEW]` marks a rule that hasn't been built yet.

```mermaid
flowchart TD
  classDef open fill:#fde2b3,stroke:#d97706,stroke-width:2px
  classDef noise fill:#e5e7eb,stroke:#6b7280
  classDef gov fill:#dbeafe,stroke:#2563eb
  classDef needle fill:#dcfce7,stroke:#16a34a,stroke-width:2px

  S0["Prep each snapshot [v1]<br/>filter FIPS, drop action flag D<br/>district_set = all districts per CLIP, uppercased, punctuation stripped [v1/v2]<br/>0 or blank = missing; multi-value setbacks parsed [v3/v5]"]
  S0 --> J{"CLIP in both snapshots? [v1]"}
  J -- "no" --> L{"Parcel lineage match?<br/>child CLIP to parent CLIP [NEW]"}:::open
  L -- "no" --> X1["Coverage churn<br/>ADDED / REMOVED"]:::noise
  L -- "yes" --> SPL["Compare parent PREV vs child CURR<br/>is_split = true (developer signal) [NEW]"]:::open
  SPL --> Z
  J -- "yes" --> Z{"Zoning present on both sides? [v2]"}
  Z -- "one side null" --> ZA["ZONING_ASSIGNED / DROPPED<br/>open: annexation vs vendor coverage"]:::open
  Z -- "yes" --> D{"district_set changed<br/>after normalization? [v2]"}
  D -- "no" --> R["Not a rezoning [v1-v5]<br/>formatting (PD vs P-D) · vendor zid change<br/>vendor data fill (null to value) · lot geometry<br/>rule updates: height / density / setback"]:::noise
  D -- "yes" --> RG{"Regulation changed AND<br/>50%+ of its CLIPs changed district? [v2]"}
  RG -- "yes" --> RGX["CODE_REGIME_SWITCH<br/>rezonings not measurable (Orange Code 2050)<br/>open: older snapshot or vendor answer"]:::gov
  RG -- "no" --> RN{"Rename or merge?<br/>crosswalk share 0.9+, n 20+ [v2]<br/>applied per district in split-zoned sets [v6b]"}
  RN -- "yes" --> RNX["RENAME_OR_MERGE"]:::gov
  RN -- "no" --> SB{"Split-zoned parcel<br/>only lost a district? [v2/v6b]"}
  SB -- "yes" --> SBX["SLIVER_DROP<br/>boundary noise"]:::noise
  SB -- "no" --> CP{"Target district is new, OR received rezonings<br/>from 4+ districts in 10+ places,<br/>mostly on non-AG land? [v6b]"}:::open
  CP -- "yes" --> CPX["CITY_PROGRAM<br/>city-led remap"]:::gov
  CP -- "no" --> KN{"Of the 20 nearest sites (same-point condos excluded),<br/>did 3 or fewer make the same change? [v6]"}:::open
  KN -- "yes" --> NP["NEEDLE_PARCEL"]:::needle
  KN -- "no" --> TR{"Same-change CLIPs<br/>within 1 km: 50 or fewer? [v6]"}:::open
  TR -- "yes" --> ST["SMALL_TRACT<br/>developer tract"]:::needle
  TR -- "no" --> MC["MASS_CHANGE"]:::gov
  NP --> EN["Enrich each candidate [v4/v6/NEW]<br/>prev to curr code · partial rezone flag<br/>annexation flag (county to city reg)<br/>direction: measured / inferred from use class / unknown<br/>lineage split flag"]:::open
  ST --> EN
  EN --> QA["QA and confirmation, lagging signals [NEW]<br/>later land-use change, new permits"]:::open
```

### Columns and rules to build

| Column / rule | Purpose | Tag | Status |
|---|---|---|---|
| `prev_dset`, `curr_dset` | Normalized, sorted set of districts per CLIP | v1/v2 | Built. **Open:** normalize RSTD/RESTRICTED prefixes |
| `lineage_parent_clip`, `is_split` | Recover rezoned and subdivided parcels | NEW | **Needs the parcel lineage product** |
| `reg_district_chg_share` | Detect code-regime switches | v2 | Built. **Open:** handling for jurisdictions that aren't measurable |
| `rename_map` (crosswalk share) | Renames and merges, including within split-zoned sets | v2/v6b | Built |
| `is_sliver_drop`, `is_partial_rezone` | Split-zoned boundary noise vs a partial rezoning | v6b | Draft |
| `tgt_is_new_district`, `tgt_n_src`, `tgt_n_clusters`, `tgt_ag_src_share` | City-program detection | v6b | Draft. **Thresholds to be set** |
| `n_colocated`, `k20_same_tr`, `n_same_tr_1km` | Needle vs tract vs mass change | v6 | Built. **Thresholds to be set through QA** |
| `is_annexation` | Developer signal | v6 | Built. **Open:** detect county→city regulation explicitly |
| `prev_use_class`, `curr_use_class`, `direction` | Up/downzone | v2/v4 | Built. **Open:** transect codes are misclassified |
| Parsed rule values (0 = missing) | Only for reporting non-rezoning change | v3/v5 | Built. Not needed for needles |