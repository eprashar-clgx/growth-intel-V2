---
name: domain-glossary
description: Maintain and apply the repo's shared vocabulary in docs/glossary.md. Use when writing or reviewing docs/SQL/issues, when a new domain concept or naming question comes up, or to check files for non-canonical terms.
---

# Domain glossary

`docs/glossary.md` is the single source of truth for domain vocabulary across both streams (`zoning/`, `growth_intel/`). It defines each concept's canonical term, its known aliases to avoid, and its exact code name.

## Apply (every time you write or review)

1. Read `docs/glossary.md` before writing docs, SQL comments, issues or PR text.
2. Use canonical terms and exact code names. Don't coin synonyms or invent new labels or column names for concepts that already exist.
3. If a concept you need isn't in the glossary, use **Capture** below. Don't silently introduce a new word.

## Capture (new or changed concept)

Use when a chat introduces a new concept, when two words are being used for one idea, or when a term's meaning changes.

1. Check whether the concept already exists under another name. If it does, use that term and offer to add the new word to **Avoid**.
2. Propose the row to the user: **Term** (canonical), **Definition** (one sentence), **Avoid** (backticked exact aliases), **Code** (exact column/label/table), **Source** (rule ID such as `R12`, or the file). Wait for the user to confirm the canonical term.
3. Add the row to the right section (Shared / Zoning / Growth Intel). If it is a new entity or relationship, update the Mermaid domain model too.
4. When renaming a term, move the old name into **Avoid**. Don't delete it.
5. Glossary edits follow the `issue-workflow` skill: make them on the current issue's branch if related, otherwise open a `repo` issue.

### Row format rules
- Put aliases in backticks, comma-separated. The linter reads only backticked text in the **Avoid** column.
- Only list aliases that are always wrong. Context-dependent words such as "rezone" (a valid verb) belong in the definition, not in **Avoid**.
- Escape pipes in cells as `\|`.

## Lint (consistency check)

```bash
python3 .github/skills/domain-glossary/lint_glossary.py            # all tracked .md/.sql/.yml
python3 .github/skills/domain-glossary/lint_glossary.py zoning/    # one folder or file
```

The script prints `file:line: 'alias' -> use 'Canonical'` and exits 1 on any hit.

- Before opening a PR, lint the files you changed and fix any hits in them.
- Renaming hits in files outside the current issue's scope (especially SQL columns or labels that downstream tables depend on) is out of scope. Report them, or open a follow-up issue, instead of fixing them on the current branch.
