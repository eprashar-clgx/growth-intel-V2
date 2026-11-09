# growth-intel-V2

BigQuery SQL and documentation for CoreLogic growth signals, organised into two independent streams of work.

| Stream | Folder | Status | What it is |
|---|---|---|---|
| Growth Intelligence V2 | [`growth_intel/`](growth_intel/) | Legacy / maintenance | Base table + V2 indicator logic (land use, new CLIP, new construction permit, builder/developer) |
| Zoning change indicator | [`zoning/`](zoning/) | Active POC | Detect developer-driven parcel rezonings from Gridics zoning snapshots |

```
growth_intel/
  README.md            indicator rules and requested changes
  sql/                 base_table.sql, V2_indicators.sql
zoning/
  README.md            stream index
  docs/zoning_POC.md   rule catalogue (R1–R15) and decision flow
  sql/                 POC and MVP queries
  sql/scratch/         throwaway / exploratory queries
```

## Contribution workflow

All new work follows **issue → branch → PR → squash merge**:

1. **Open an issue** using a template (Growth Intel / Zoning / Repo). Label it with its stream.
2. **Branch from `main`** named `<issue#>-<short-slug>`, e.g. `12-zoning-sliver-rule`.
3. **Commit** on the branch; reference the issue (`Refs #12`).
4. **Open a PR** into `main` whose description contains `Closes #12`.
5. **Squash merge** once the issue's "done when" criteria are met. The branch is deleted automatically and the issue closes.

Never commit directly to `main`. Keep each PR scoped to one issue and one stream.

With the GitHub CLI:

```bash
gh issue create --label zoning --title "..."          # 1
git switch main && git pull && git switch -c 12-slug   # 2
gh pr create --fill --body "Closes #12"                # 4
gh pr merge --squash --delete-branch                   # 5
```

Copilot CLI users can ask for the `issue-workflow` skill (`.github/skills/issue-workflow/`) to drive these steps.
