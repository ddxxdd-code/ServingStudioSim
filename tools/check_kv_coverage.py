"""Require added executable Rust and Python lines to meet the KV coverage target.

Rust counts come from LLVM LCOV; Python counts come from coverage.py JSON.
Added lines are resolved against the original distributed-KV base, including
untracked source files. Test modules/bodies are excluded from the denominator.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

PYTHON_SOURCES = {
    "launcher/schema/loader.py",
    "launcher/schema/validate.py",
    "tools/validate_distributed_kv.py",
    "tools/generate_kv_storage_oracles.py",
    "tools/check_kv_coverage.py",
    "tests/fixtures/gen_distributed_kv_trace.py",
}


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout


def added_lines(root: Path, base: str) -> dict[str, set[int]]:
    result: dict[str, set[int]] = {}
    path = None
    line = 0
    for text in git(root, "diff", "--no-ext-diff", "--unified=0", base, "--").splitlines():
        if text.startswith("+++ b/"):
            path = text[6:]
            result[path] = set()
        elif text.startswith("@@"):
            match = re.search(r"\+(\d+)(?:,\d+)? @@", text)
            if match:
                line = int(match[1])
        elif text.startswith("+") and not text.startswith("+++") and path:
            result[path].add(line)
            line += 1
        elif text.startswith(" "):
            line += 1
    for path in git(root, "ls-files", "--others", "--exclude-standard").splitlines():
        if path.endswith((".rs", ".py")):
            result[path] = set(range(1, len((root / path).read_text().splitlines()) + 1))
    return result


def rust_test_lines(source: str) -> set[int]:
    # Replace strings/comments with spaces while retaining line numbers and
    # braces in actual Rust code. This also handles nested braces in test bodies.
    cleaned = re.sub(
        r'//[^\n]*|/\*[\s\S]*?\*/|r(#+)?"[\s\S]*?"\1|"(?:\\[\s\S]|[^"\\])*"',
        lambda match: re.sub(r"[^\n]", " ", match.group()),
        source,
    )
    excluded: set[int] = set()
    for match in re.finditer(r"#\[cfg\(test\)\]", cleaned):
        opening = cleaned.find("{", match.end())
        if opening < 0:
            continue
        depth, end = 1, opening + 1
        while end < len(cleaned) and depth:
            depth += (cleaned[end] == "{") - (cleaned[end] == "}")
            end += 1
        start_line = cleaned.count("\n", 0, match.start()) + 1
        end_line = cleaned.count("\n", 0, end) + 1
        excluded.update(range(start_line, end_line + 1))
    return excluded


def read_lcov(path: Path, root: Path) -> dict[str, dict[int, int]]:
    result: dict[str, dict[int, int]] = {}
    current = None
    for text in path.read_text().splitlines():
        if text.startswith("SF:"):
            source = Path(text[3:])
            try:
                name = str(source.resolve().relative_to(root.resolve()))
            except ValueError:
                current = None
                continue
            current = result.setdefault(name, {})
        elif text.startswith("DA:") and current is not None:
            line, count, *_ = text[3:].split(",")
            current[int(line)] = max(current.get(int(line), 0), int(count))
    return result


def build_report(root: Path, base: str, rust: dict, python: dict, minimum: float) -> dict:
    changed = added_lines(root, base)
    base_files = set(git(root, "ls-tree", "-r", "--name-only", base).splitlines())
    files = []
    for name, lines in sorted(changed.items()):
        if name.endswith("_tests.rs"):
            continue
        if name.endswith(".rs") and name.startswith(("simulator/src/", "analyzer/rust/src/")):
            source = (root / name).read_text()
            test_lines = rust_test_lines(source)
            lines -= test_lines
            if not lines:
                continue
            production = "\n".join(
                text
                for number, text in enumerate(source.splitlines(), 1)
                if number not in test_lines
            )
            if name not in rust and re.search(r"\bfn\s+[^;{]*\{", production):
                raise ValueError(f"missing Rust coverage for {name}")
            executable = lines & rust.get(name, {}).keys()
            covered = {line for line in executable if rust[name][line] > 0}
            language = "rust"
        elif name in PYTHON_SOURCES:
            data = python.get("files", {}).get(name)
            if data is None:
                raise ValueError(f"missing Python coverage for {name}")
            covered = lines & set(data["executed_lines"])
            executable = covered | (lines & set(data["missing_lines"]))
            language = "python"
        else:
            continue
        if executable:
            files.append(
                {
                    "file": name,
                    "language": language,
                    "new_file": name not in base_files,
                    "added_executable": len(executable),
                    "covered": len(covered),
                    "percent": 100 * len(covered) / len(executable),
                    "missing_lines": sorted(executable - covered),
                }
            )
    totals = {}
    for language in ("rust", "python"):
        selected = [file for file in files if file["language"] == language]
        total = sum(file["added_executable"] for file in selected)
        covered = sum(file["covered"] for file in selected)
        if total == 0:
            raise ValueError(f"no added executable {language} lines measured")
        totals[language] = {
            "added_executable": total,
            "covered": covered,
            "percent": 100 * covered / total,
        }
    return {
        "base": git(root, "rev-parse", base).strip(),
        "minimum_percent": minimum,
        "passed": all(value["percent"] >= minimum for value in totals.values())
        and all(file["percent"] >= minimum for file in files if file["new_file"]),
        "totals": totals,
        "files": files,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="48864532c291e5cd48d01efb515d63733496e420")
    parser.add_argument("--rust-lcov", required=True, type=Path)
    parser.add_argument("--python-json", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--minimum", default=80.0, type=float)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    report = build_report(
        root,
        args.base,
        read_lcov(args.rust_lcov, root),
        json.loads(args.python_json.read_text()),
        args.minimum,
    )
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    for language, total in report["totals"].items():
        print(
            f"{language}: {total['covered']}/{total['added_executable']} added lines "
            f"= {total['percent']:.2f}% (required {args.minimum:.0f}%)"
        )
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
