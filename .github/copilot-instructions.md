# Copilot instructions

## Repo layout
- `growth_intel/` — legacy Growth Intelligence V2 indicators (`sql/base_table.sql`, `sql/V2_indicators.sql`). Maintenance only.
- `zoning/` — active zoning change POC. Rules are documented in `zoning/docs/zoning_mvp2.md` (active MVP2 design) and `zoning/docs/zoning_POC_mvp1.md` (MVP1, frozen); SQL in `zoning/sql/`; exploratory queries in `zoning/sql/scratch/`.
- Keep the two streams separate: a change should touch only one stream folder unless the issue says otherwise.

## Vocabulary
- `docs/glossary.md` is the shared domain vocabulary. Read it before writing docs, SQL comments, issues or PRs; use its canonical terms and exact code names, and never coin synonyms.
- New or renamed concepts go into the glossary via the `domain-glossary` skill (`.github/skills/domain-glossary/SKILL.md`).

## SQL conventions
- All queries are BigQuery Standard SQL scripts; parameters are `DECLARE`d at the top (e.g. `fips_list`).
- FIPS codes are normalised with `LPAD(fips_code, 5, '0')`.
- When SQL logic implements or changes a rule, reference its rule ID (e.g. `-- R8`) and update `zoning/docs/zoning_mvp2.md` (or `zoning/docs/zoning_POC_mvp1.md` for an MVP1 bug fix) in the same PR.

## Workflow
- Every change starts from a GitHub issue and lands via a PR from a branch named `<issue#>-<slug>`; never commit to `main`.
- PRs are squash-merged and must contain `Closes #<issue#>`.
- Use the `issue-workflow` skill (`.github/skills/issue-workflow/SKILL.md`) for the full procedure.
