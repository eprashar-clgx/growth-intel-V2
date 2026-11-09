# growth-intel-V2

BigQuery SQL and documentation for CoreLogic growth signals, organised into two independent streams of work.

| Stream | Folder | Status | What it is |
|---|---|---|---|
| Growth Intelligence V2 | [`growth_intel/`](growth_intel/) | Legacy / maintenance | Base table + V2 indicator logic (land use, new CLIP, new construction permit, builder/developer) |
| Zoning change indicator | [`zoning/`](zoning/) | Active POC | Detect developer-driven parcel rezonings from Gridics zoning snapshots |

```
docs/
  glossary.md          shared vocabulary and domain model (both streams)
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

0. **Plan a batch** of related items as a parent *tracking issue* with one *sub-issue* per item. Size each sub-issue as one independently mergeable PR with one clear "Done when"; the parent closes when all sub-issues are done.
1. **Open an issue** using a template (Growth Intel / Zoning / Repo). Label it with its stream.
2. **Branch from `origin/main`** in its own git worktree, named `<issue#>-<short-slug>`, e.g. `12-zoning-sliver-rule`.
3. **Commit** on the branch; reference the issue (`Refs #12`).
4. **Open a PR** into `main` whose description contains `Closes #12`. Rebase onto `origin/main` first if it has moved.
5. **Squash merge** once the issue's "done when" criteria are met. The remote branch is deleted automatically and the issue closes. Then remove the worktree.

Never commit directly to `main`. Keep each PR scoped to one issue and one stream.

With the GitHub CLI:

```bash
gh issue create --label zoning --title "..."                                          # 1
git fetch origin && git worktree add ../growth-intel-V2.worktrees/12-slug -b 12-slug origin/main  # 2
gh pr create --fill --body "Closes #12"                                               # 4
gh pr merge --squash                                                                  # 5
git worktree remove ../growth-intel-V2.worktrees/12-slug && git branch -D 12-slug     # 5, from the root checkout
```

### Working on several issues in parallel

The root checkout stays on `main` for planning. Each issue gets its own **worktree** (a sibling folder on its own branch) and its own **Copilot session**:

```
growth-intel-V2/                            main: planning session
growth-intel-V2.worktrees/9-city-program/   branch 9-city-program: session "9-city-program"
growth-intel-V2.worktrees/10-change-table/  branch 10-change-table: session "10-change-table"
```

- Start a session in the worktree with `copilot --name <branch>`; come back to it with `copilot --resume <branch>`. Inside the CLI, `/new worktree` creates a worktree and session in one go.
- Use one terminal tab and one VS Code window per worktree.
- Sub-issues that edit the same file should run one after another, or be rebased onto `origin/main` before merging.
- Durable decisions belong in the issue, PR, glossary or docs, not only in a session's chat history.

Copilot CLI users can ask for the `issue-workflow` skill (`.github/skills/issue-workflow/`) to drive these steps.

## Vocabulary

Use the terms in [`docs/glossary.md`](docs/glossary.md) consistently in code, docs, issues and chats. Copilot's `domain-glossary` skill adds new terms and checks files for non-canonical aliases:

```bash
python3 .github/skills/domain-glossary/lint_glossary.py
```
