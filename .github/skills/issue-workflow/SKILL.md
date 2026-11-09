---
name: issue-workflow
description: Issue → branch → PR → squash-merge workflow for this repo. Use when starting new work, picking up a GitHub issue, opening a PR, or merging into main.
---

# Issue workflow

Every change to this repo follows: **GitHub issue → branch → PR → squash merge into `main`**. Use the `gh` CLI for all GitHub operations. Never commit directly to `main`.

## 1. Start: issue

If the user references an existing issue, read it with `gh issue view <N>`. Otherwise create one:

```bash
gh issue create --title "<imperative summary>" --label <stream> --body "<body>"
```

- `<stream>` label: `zoning`, `growth-intel`, or `repo` (tooling/structure). One stream per issue.
- Body must include **Goal**, **Scope** (task checklist), and **Done when** (acceptance criteria). Match the templates in `.github/ISSUE_TEMPLATE/`.
- Confirm title/scope with the user before creating if the request is ambiguous.

## 2. Branch

```bash
git switch main && git pull --ff-only
git switch -c <N>-<short-kebab-slug>
```

Abort and tell the user if the working tree is dirty with unrelated changes.

## 3. Work

- Stay inside the stream folder for the issue (`zoning/` or `growth_intel/`), unless the issue is labelled `repo`.
- Zoning rule changes: tag SQL with the rule ID (`-- R10`) and update `zoning/docs/zoning_POC.md` in the same branch.
- Use the vocabulary in `docs/glossary.md`; add new concepts there (see the `domain-glossary` skill) and lint changed files before opening the PR.
- Commit in small logical steps; end messages with `Refs #<N>`.

## 4. Pull request

```bash
git push -u origin HEAD
gh pr create --base main --title "<issue title>" --label <stream> --body "<body>"
```

PR body follows `.github/pull_request_template.md` and **must** contain `Closes #<N>`. Summarise what changed, how it was validated (e.g. BigQuery run, row counts, counties checked), and any follow-ups.

## 5. Merge

Only merge when the issue's "Done when" criteria are met and the user approves:

```bash
gh pr merge <PR#> --squash --delete-branch
git switch main && git pull --ff-only
```

Squash commit title: `<issue title> (#<PR#>)`. Verify the issue closed (`gh issue view <N>`).

## Follow-ups

Out-of-scope findings become new issues (`gh issue create`), not extra commits on the current branch.
