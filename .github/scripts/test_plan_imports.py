# ruff: noqa: INP001, S603, S607, S101, T201
"""Self-check for plan_imports.py: cd .github/scripts && python3 test_plan_imports.py."""

import subprocess
import tempfile
from pathlib import Path

from plan_imports import FULL, plan

CONFIG = """
[[states]]
name = "Himachal Pradesh"
resource_id = "{rid}"
  [[states.modules]]
  module = "flood"
  indicators = "indicators/hp.csv"
  data = "data/hp.csv"
"""


def commit(repo, files):
    for path, text in files.items():
        (repo / path).parent.mkdir(parents=True, exist_ok=True)
        (repo / path).write_text(text)
    subprocess.run(["git", "-C", repo, "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            repo,
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-qm",
            "x",
        ],
        check=True,
    )
    return subprocess.run(
        ["git", "-C", repo, "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


with tempfile.TemporaryDirectory() as tmp:
    repo = Path(tmp)
    subprocess.run(["git", "init", "-q", repo], check=True)
    base = commit(
        repo,
        {
            "config.toml": CONFIG.format(rid="a"),
            "indicators/hp.csv": "\ufeffindicatorSlug,x\nrisk,1\nrain,2\n",
            "data/hp.csv": "v\n1\n",
        },
    )
    data = commit(repo, {"data/hp.csv": "v\n2\n"})
    rid = commit(repo, {"config.toml": CONFIG.format(rid="b")})
    added = commit(
        repo, {"indicators/hp.csv": "indicatorSlug,x\nrisk,1\nrain,2\nheat,3\n"}
    )
    dropped = commit(repo, {"indicators/hp.csv": "indicatorSlug,x\nrisk,1\n"})

    assert plan(repo, base, data) == ["import_data --state 'Himachal Pradesh'"]
    assert plan(repo, data, rid) == []
    assert plan(repo, rid, added) == [
        "import_indicators --state 'Himachal Pradesh'",
        "import_data --state 'Himachal Pradesh'",
    ]
    assert plan(repo, added, dropped) == FULL
    assert plan(repo, "0" * 40, dropped) == FULL
    assert plan(repo, "deadbeef", dropped) == FULL
    print("ok")
