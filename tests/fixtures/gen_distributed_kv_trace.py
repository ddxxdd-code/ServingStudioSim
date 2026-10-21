"""Deterministic multi-turn load beyond the small distributed-KV smoke fixture."""

import argparse
import csv
from pathlib import Path


def generate(path: Path, sessions: int = 64, rounds: int = 8) -> None:
    if sessions < 1 or rounds < 2:
        raise ValueError("sessions must be positive and rounds at least two")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(
            [
                "request_id",
                "session_id",
                "round_idx",
                "arrival_time_ms",
                "prefix_len",
                "input_len",
                "output_len",
                "tool_wait_after_ms",
            ]
        )
        for session in range(sessions):
            for turn in range(rounds):
                writer.writerow(
                    [
                        f"session_{session}_round_{turn:06d}",
                        session,
                        turn,
                        0,
                        0 if turn == 0 else 32 + (turn - 1) * 8,
                        32 if turn == 0 else 8,
                        4,
                        0,
                    ]
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--sessions", type=int, default=64)
    parser.add_argument("--rounds", type=int, default=8)
    args = parser.parse_args()
    generate(args.output, args.sessions, args.rounds)
