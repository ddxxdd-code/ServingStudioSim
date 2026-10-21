"""Concrete chained-session inputs for KV reuse and storage contention tests."""

import csv
import runpy
import subprocess
import sys
from pathlib import Path

import pytest

GENERATOR = Path(__file__).parent / "fixtures/gen_distributed_kv_trace.py"


def test_two_sessions_three_turns_keep_full_context_with_eight_new_tokens(tmp_path):
    output = tmp_path / "nested/two_sessions.csv"
    subprocess.run(
        [sys.executable, str(GENERATOR), str(output), "--sessions", "2", "--rounds", "3"],
        check=True,
    )
    with output.open() as stream:
        rows = list(csv.DictReader(stream))
    assert [
        (
            row["request_id"],
            int(row["session_id"]),
            int(row["round_idx"]),
            int(row["prefix_len"]),
            int(row["input_len"]),
            int(row["output_len"]),
        )
        for row in rows
    ] == [
        ("session_0_round_000000", 0, 0, 0, 32, 4),
        ("session_0_round_000001", 0, 1, 32, 8, 4),
        ("session_0_round_000002", 0, 2, 40, 8, 4),
        ("session_1_round_000000", 1, 0, 0, 32, 4),
        ("session_1_round_000001", 1, 1, 32, 8, 4),
        ("session_1_round_000002", 1, 2, 40, 8, 4),
    ]
    assert [int(row["prefix_len"]) + int(row["input_len"]) for row in rows] == [32, 40, 48] * 2
    assert [int(row["tool_wait_after_ms"]) for row in rows] == [0] * 6


@pytest.mark.parametrize("sessions, rounds", [(0, 3), (-1, 3), (2, 1)])
def test_invalid_reuse_trace_is_rejected_before_creating_output(tmp_path, sessions, rounds):
    generate = runpy.run_path(str(GENERATOR))["generate"]
    output = tmp_path / "invalid.csv"
    with pytest.raises(ValueError, match="sessions must be positive and rounds at least two"):
        generate(output, sessions, rounds)
    assert output.exists() is False
