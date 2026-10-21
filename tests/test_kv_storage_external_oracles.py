"""Provenance and optional regeneration checks for external KV oracles."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tools import generate_kv_storage_oracles as oracles

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "tests/fixtures/kv_storage_external_oracles.json"
GENERATOR = ROOT / "tools/generate_kv_storage_oracles.py"


def test_external_oracle_fixture_is_pinned_and_explicitly_scoped():
    fixture = json.loads(FIXTURE.read_text())
    assert fixture["schema_version"] == 1
    assert fixture["mooncake"]["commit"] == oracles.MOONCAKE_COMMIT
    assert fixture["kvcached"]["commit"] == oracles.KVCACHED_COMMIT
    assert fixture["mooncake"]["observations"] == {
        "lru_evictions": ["key1", "key4"],
        "fifo_evictions": ["key1", "key2"],
    }
    assert fixture["kvcached"]["capacity"] == 8
    assert [
        (row["accepted"], row["success"], row["used"], row["free"])
        for row in fixture["kvcached"]["observations"]
    ] == [(5, True, 5, 3), (0, False, 5, 3), (5, True, 0, 8), (8, True, 8, 0)]
    assert fixture["scope"]["exact"] == [
        "Mooncake LRU/FIFO victim order",
        "kvcached all-or-nothing allocation and release accounting",
    ]


def test_external_oracle_regeneration_is_exact_when_checkouts_are_supplied():
    mooncake = os.environ.get("MOONCAKE_CHECKOUT")
    kvcached = os.environ.get("KVCACHED_CHECKOUT")
    if not mooncake or not kvcached:
        pytest.skip("set MOONCAKE_CHECKOUT and KVCACHED_CHECKOUT to rerun pinned upstream code")
    output = subprocess.run(
        [
            sys.executable,
            str(GENERATOR),
            "--mooncake",
            mooncake,
            "--kvcached",
            kvcached,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert json.loads(output) == json.loads(FIXTURE.read_text())


def test_oracle_refuses_reference_repository_at_the_wrong_revision(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "example").write_text("unreviewed reference version")
    subprocess.run(["git", "add", "example"], cwd=tmp_path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Oracle Test",
            "-c",
            "user.email=oracle@example.invalid",
            "commit",
            "-qm",
            "unreviewed",
        ],
        cwd=tmp_path,
        check=True,
    )
    actual = oracles._commit(tmp_path)
    with pytest.raises(SystemExit, match=f"checkout is {actual}; expected pinned"):
        oracles._require_commit(tmp_path, oracles.MOONCAKE_COMMIT, "Mooncake")


def test_oracle_rejects_an_unexpected_mooncake_dependency_instead_of_running_a_substitute(
    tmp_path, monkeypatch
):
    # An upstream header with a renamed dependency cannot safely use the
    # reviewed ErrorCode adapter. Stop before compilation or execution.
    include = tmp_path / "mooncake-store/include"
    include.mkdir(parents=True)
    (include / "eviction_strategy.h").write_text('#pragma once\n#include "new_types.h"\n')
    monkeypatch.setattr(oracles, "_commit", lambda _path: oracles.MOONCAKE_COMMIT)
    with pytest.raises(SystemExit, match="unexpected types include"):
        oracles._mooncake_observations(tmp_path)
