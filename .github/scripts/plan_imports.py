# ruff: noqa: INP001, S603, S607, T201
"""
Print the manage.py import commands (";"-separated) that a plugin change needs.

Usage: plan_imports.py PLUGIN_GIT_DIR BEFORE_SHA AFTER_SHA

import_geojson and import_indicators upsert, and import_data replaces a state's
values, so only removals need delete_imports + a full re-import.
"""

import csv
import io
import shlex
import subprocess
import sys
import tomllib

FULL = [
    "delete_imports --noinput",
    "import_geojson",
    "import_indicators",
    "import_data",
]
FULL_KEYS = {"geojson", "simplify_tolerance", "snap_to_grid_size"}
IGNORED_STATE_KEYS = {"resource_id", "hidden"}


def git(repo, *args):
    return subprocess.run(
        ["git", "-C", repo, *args], capture_output=True, text=True, check=True
    ).stdout


def show(repo, sha, path):
    try:
        return git(repo, "show", f"{sha}:{path}")
    except subprocess.CalledProcessError:
        return None


def slugs(text):
    rows = csv.DictReader(
        io.StringIO((text or "").lstrip("\ufeff"))
    )  # pandas strips the BOM too
    return {(row.get("indicatorSlug") or "").strip() for row in rows}


def _significant(spec):
    return {k: v for k, v in (spec or {}).items() if k not in IGNORED_STATE_KEYS}


def plan(repo, before, after):
    if not before or set(before) == {"0"}:
        return FULL
    try:
        changed = git(repo, "diff", "--name-only", before, after).split()
    except subprocess.CalledProcessError:  # e.g. before was force-pushed away
        return FULL

    old = tomllib.loads(show(repo, before, "config.toml") or "")
    new = tomllib.loads(show(repo, after, "config.toml") or "")
    if any(old.get(k) != new.get(k) for k in FULL_KEYS) or any(
        p.startswith("geography/") for p in changed
    ):
        return FULL

    old_states = {s["name"]: s for s in old.get("states", [])}
    new_states = {s["name"]: s for s in new.get("states", [])}
    if old_states.keys() - new_states.keys():
        return FULL

    indicators, data = set(), set()
    for name, spec in new_states.items():
        if _significant(spec) != _significant(old_states.get(name)):
            indicators.add(name)
        for module in spec.get("modules", []):
            path = module["indicators"]
            if path in changed:
                if slugs(show(repo, before, path)) - slugs(show(repo, after, path)):
                    return (
                        FULL  # an indicator was dropped; upsert would leave it behind
                    )
                indicators.add(name)
            if module["data"] in changed:
                data.add(name)

    steps = [
        f"import_indicators --state {shlex.quote(name)}" for name in sorted(indicators)
    ]
    if old.get("whitelist_indicators") != new.get("whitelist_indicators"):
        return [*steps, "import_data"]
    return steps + [
        f"import_data --state {shlex.quote(name)}" for name in sorted(indicators | data)
    ]


if __name__ == "__main__":
    print(";".join(plan(*sys.argv[1:4])))
