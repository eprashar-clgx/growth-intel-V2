#!/usr/bin/env python3
"""Flag 'Avoid' aliases from docs/glossary.md in repo files.

Usage: python3 .github/skills/domain-glossary/lint_glossary.py [PATH ...]
With no paths, scans all git-tracked .md/.sql/.yml files. Exits 1 if any alias is found.
"""
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
GLOSSARY = ROOT / "docs" / "glossary.md"
SUFFIXES = {".md", ".sql", ".yml", ".yaml"}
EXCLUDE = {GLOSSARY, ROOT / ".github" / "skills" / "domain-glossary"}


def cells(line):
    return [c.strip() for c in re.split(r"(?<!\\)\|", line.strip().strip("|"))]


def load_aliases():
    aliases, term_i, avoid_i = {}, None, None
    for line in GLOSSARY.read_text().splitlines():
        if not line.lstrip().startswith("|"):
            term_i = avoid_i = None
            continue
        row = cells(line)
        if "Avoid" in row and "Term" in row:
            term_i, avoid_i = row.index("Term"), row.index("Avoid")
            continue
        if avoid_i is None or len(row) <= avoid_i or set(row[0]) <= set("-: "):
            continue
        canonical = re.sub(r"[*`]", "", row[term_i])
        for alias in re.findall(r"`([^`]+)`", row[avoid_i]):
            aliases[alias] = canonical
    return aliases


def target_files(args):
    if args:
        paths = []
        for a in args:
            p = Path(a).resolve()
            paths += [f for f in p.rglob("*") if f.is_file()] if p.is_dir() else [p]
    else:
        out = subprocess.check_output(["git", "ls-files"], cwd=ROOT, text=True)
        paths = [ROOT / f for f in out.splitlines()]
    return [p for p in paths
            if p.suffix in SUFFIXES and not any(p == e or e in p.parents for e in EXCLUDE)]


def main():
    aliases = load_aliases()
    patterns = {
        a: re.compile(r"(?<![A-Za-z0-9_])" + r"\s+".join(map(re.escape, a.split())) + r"(?![A-Za-z0-9_])", re.I)
        for a in aliases
    }
    hits = 0
    for path in sorted(target_files(sys.argv[1:])):
        for n, text in enumerate(path.read_text(errors="ignore").splitlines(), 1):
            for alias, pat in patterns.items():
                for m in pat.finditer(text):
                    hits += 1
                    print(f"{path.relative_to(ROOT)}:{n}: '{m.group(0)}' -> use '{aliases[alias]}'")
    print(f"{hits} alias hit(s), {len(aliases)} aliases checked", file=sys.stderr)
    return 1 if hits else 0


if __name__ == "__main__":
    sys.exit(main())
