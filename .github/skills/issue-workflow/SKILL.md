---
name: issue-workflow
description: Plan → issue → branch → PR → squash-merge workflow for this repo. Use when planning a batch of work, starting new work, picking up a GitHub issue, opening a PR, or merging into main.
---

# Issue workflow

Every change to this repo follows: **GitHub issue → branch → PR → squash merge into `main`**. Use the `gh` CLI for all GitHub operations. Never commit directly to `main`.

## 0. Plan a batch (when the user lists several work items)

1. **Size the items.** One issue = one PR that can be reviewed in one sitting, has one clear "Done when", and can merge on its own. Merge items that share a source and change (e.g. two columns from the same join). Split items that touch different sources or rules.
2. **Order them.** Note dependencies between items. If one item changes the table that others write to, say so in the issues and confirm the order with the user.
   Also flag sub-issues that will edit the **same file** (e.g. several changes to one SQL script). They should run one after another, or each needs a rebase before merging. Sub-issues that touch different files can run in parallel.
3. **Propose the grouping to the user and confirm before creating anything.**
4. **Create a parent tracking issue** (title ends with `(tracking issue)`). Its body holds the batch Goal, a summary of the sub-issues and their order, and "Done when: all sub-issues closed".
5. **Create each sub-issue** as in step 1, including its **Context brief** (see below), then link it to the parent:

   ```bash
   id=$(gh api repos/{owner}/{repo}/issues/<child#> --jq .id)
   gh api -X POST repos/{owner}/{repo}/issues/<parent#>/sub_issues -F sub_issue_id=$id
   gh api repos/{owner}/{repo}/issues/<parent#>/sub_issues --jq '.[].number'   # verify
   ```

6. Each sub-issue then goes through steps 1–5 independently. Close the parent once all its sub-issues are closed.

New domain terms mentioned in the items (data sources, flags, tables) belong in the sub-issue's scope as a glossary update.

### Context brief

The planning session has already read the docs and made decisions. A brief in each issue hands that over, so the issue's session doesn't start from scratch. Write it under a `### Context` heading in the issue body:

- **Read first:** exact files and line ranges, and what the session can skip.
- **Tables and joins:** source tables, keys, grain, and prior art in the repo.
- **Decisions:** what's already settled with the user, and column and term names to use (from the glossary).
- **Gotchas:** other issues that edit the same file, access limits, and **open questions** the session must ask the user before writing code.

Point to files rather than copying them, and keep it short (around 20 lines). Only write down facts you checked in this session; label anything unverified as an open question. If a brief turns out to be wrong after the issue starts, edit the issue (`gh issue edit <N> --body-file …`) and tell the user to have the issue's session re-read it.

## 1. Start: issue

If the user references an existing issue, read it with `gh issue view <N>`. Otherwise create one:

```bash
gh issue create --title "<imperative summary>" --label <stream> --body "<body>"
```

- `<stream>` label: `zoning`, `growth-intel`, or `repo` (tooling/structure). One stream per issue.
- Body must include **Goal**, **Scope** (task checklist), and **Done when** (acceptance criteria). Match the templates in `.github/ISSUE_TEMPLATE/`.
- Add a **Context** brief (see step 0) whenever this session has context the next one would otherwise have to rediscover.
- Confirm title/scope with the user before creating if the request is ambiguous.

## 2. Branch (one worktree + one session per issue)

The root checkout stays on `main` and is used for planning. Each issue gets its own git worktree, a sibling folder on its own branch, so several issues can be in progress at once without interfering.

The user normally starts an issue with `start-issue <N>` (`bin/start-issue`). It creates the worktree from `origin/main`, opens VS Code, and launches a session named `<N>-<slug>` that is already in the worktree. In that case, skip this step. To start an issue for the user from another session (e.g. the planning session), run `bin/start-issue <N> --no-code --no-copilot` from the repo and tell them to run `start-issue <N>`, which resumes it. The manual equivalent is:

```bash
git fetch origin
git worktree add ../<repo>.worktrees/<N>-<slug> -b <N>-<slug> origin/main
```

- Work in that folder in a dedicated Copilot session named after the branch: `cd ../<repo>.worktrees/<N>-<slug> && copilot --name <N>-<slug>`; resume it later with `copilot --resume <N>-<slug>`.
- If the current session is already inside the issue's worktree, skip this step.
- Never `git switch main` inside a worktree: `main` is checked out in the root folder, so git refuses.
- Abort and tell the user if the working tree is dirty with unrelated changes.

## 3. Work

- **Start from the brief.** Read the issue (`gh issue view <N>`) and its Context section first, then open only the files and line ranges it lists. Resolve its open questions with the user before writing code. Widen the search only when the brief falls short, and note any gap for the PR's "Brief feedback". If there's no brief, read `.github/copilot-instructions.md` and the stream's README first.
- Stay inside the stream folder for the issue (`zoning/` or `growth_intel/`), unless the issue is labelled `repo`.
- Zoning rule changes: tag SQL with the rule ID (`-- R10`) and update `zoning/docs/zoning_mvp2.md` (or `zoning/docs/zoning_POC_mvp1.md` for an MVP1 bug fix) in the same branch.
- Use the vocabulary in `docs/glossary.md`; add new concepts there (see the `domain-glossary` skill) and lint changed files before opening the PR.
- Commit in small logical steps; end messages with `Refs #<N>`.

## 4. Pull request

If `main` moved since the branch was created (e.g. another sub-issue merged), rebase first and re-validate:

```bash
git fetch origin && git rebase origin/main
```

```bash
git push -u origin HEAD
gh pr create --base main --title "<issue title>" --label <stream> --body "<body>"
```

PR body follows `.github/pull_request_template.md` and **must** contain `Closes #<N>`. Summarise what changed, how it was validated (e.g. BigQuery run, row counts, counties checked), and any follow-ups. Under "Brief feedback", note what the issue's brief missed or got wrong, or "none".

## 5. Merge

Only merge when the issue's "Done when" criteria are met and the user approves:

```bash
gh pr merge <PR#> --squash        # repo auto-deletes the remote branch
```

Then clean up from the **root checkout**, not from inside the worktree:

```bash
cd <root checkout>
git worktree remove ../<repo>.worktrees/<N>-<slug>
git branch -D <N>-<slug>          # -D: squash-merged branches aren't ancestors of main
git pull --ff-only && git fetch --prune
```

Squash commit title: `<issue title> (#<PR#>)`. Verify the issue closed (`gh issue view <N>`). Tell the user that the issue's session can now be closed.

## Follow-ups

Out-of-scope findings become new issues (`gh issue create`), not extra commits on the current branch.
