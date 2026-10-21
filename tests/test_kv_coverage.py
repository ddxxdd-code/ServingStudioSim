"""Coverage accounting must fail below the target and ignore test-only hits."""

import json
import subprocess

import pytest

from tools import check_kv_coverage as checker


def example_repo(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "README.md").write_text("Coverage example\n")
    subprocess.run(["git", "add", "README.md"], cwd=tmp_path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Coverage Test",
            "-c",
            "user.email=coverage@example.invalid",
            "commit",
            "-qm",
            "base",
        ],
        cwd=tmp_path,
        check=True,
    )
    (tmp_path / "simulator/src").mkdir(parents=True)
    (tmp_path / "tools").mkdir()
    source = """fn available(capacity: u64, used: u64) -> u64 {
    capacity.saturating_sub(used)
}
#[cfg(test)]
mod tests {
    #[test]
    fn full_store() { assert_eq!(available(8, 8), 0); }
}
"""
    (tmp_path / "simulator/src/store.rs").write_text(source)
    (tmp_path / "tools/validate_distributed_kv.py").write_text("used = 5\nfree = 8 - used\n")
    return source


def test_added_line_gate_cannot_be_inflated_by_covered_rust_test_code(tmp_path):
    example_repo(tmp_path)
    rust = {"simulator/src/store.rs": {1: 1, 2: 0, 3: 1, 7: 1000}}
    python = {
        "files": {
            "tools/validate_distributed_kv.py": {"executed_lines": [1, 2], "missing_lines": []}
        }
    }
    report = checker.build_report(tmp_path, "HEAD", rust, python, 80)
    assert report["passed"] is False
    assert report["totals"]["rust"] == {"added_executable": 3, "covered": 2, "percent": 200 / 3}
    assert report["totals"]["python"]["percent"] == 100
    assert report["files"][0]["missing_lines"] == [2]
    rust["simulator/src/store.rs"][2] = 1
    assert checker.build_report(tmp_path, "HEAD", rust, python, 80)["passed"] is True


def test_lcov_aggregates_duplicate_hits_and_drops_external_dependency_paths(tmp_path):
    example_repo(tmp_path)
    source = tmp_path / "simulator/src/store.rs"
    lcov = tmp_path / "coverage.lcov"
    lcov.write_text(
        f"SF:{source}\nDA:1,0\nDA:2,0\nend_of_record\n"
        f"SF:{source}\nDA:1,3\nDA:2,0\nend_of_record\n"
        "SF:/external/dependency.rs\nDA:1,999\nend_of_record\n"
    )
    assert checker.read_lcov(lcov, tmp_path) == {"simulator/src/store.rs": {1: 3, 2: 0}}


def test_rust_test_filter_handles_braces_inside_comments_and_string_literals():
    source = """fn value() -> u64 { 8 }
#[cfg(test)]
fn check_value() {
    // } is not the end of a block.
    let message = "a } brace";
    if value() == 8 { assert_eq!(message, "a } brace"); }
}
fn production_after_test() -> u64 { 9 }
"""
    assert checker.rust_test_lines(source) == set(range(2, 8))


def test_rust_test_filter_handles_a_multiline_escaped_production_string():
    source = "\n".join(
        [
            'fn error() { println!("capacity ' + chr(92),
            'exceeded"); }',
            "#[cfg(test)]",
            "mod tests {",
            "    fn test_capacity() { error(); }",
            "}",
        ]
    )
    assert checker.rust_test_lines(source) == {3, 4, 5, 6}


def test_missing_language_reports_fail_instead_of_becoming_vacuous_success(tmp_path):
    example_repo(tmp_path)
    rust = {"simulator/src/store.rs": {1: 1, 2: 1, 3: 1}}
    with pytest.raises(ValueError, match="missing Python coverage"):
        checker.build_report(tmp_path, "HEAD", rust, {"files": {}}, 80)
    python = {
        "files": {
            "tools/validate_distributed_kv.py": {"executed_lines": [], "missing_lines": [1, 2]}
        }
    }
    with pytest.raises(ValueError, match="missing Rust coverage"):
        checker.build_report(tmp_path, "HEAD", {}, python, 80)
    with pytest.raises(ValueError, match="no added executable rust"):
        checker.build_report(tmp_path, "HEAD", {"simulator/src/store.rs": {}}, python, 80)


@pytest.mark.parametrize("count, exit_code", [(0, 1), (1, 0)])
def test_cli_writes_exact_counts_and_returns_failure_below_threshold(
    tmp_path, monkeypatch, capsys, count, exit_code
):
    example_repo(tmp_path)
    # Make __file__ point at the example repository, just like a standalone run.
    monkeypatch.setattr(checker, "__file__", str(tmp_path / "tools/check_kv_coverage.py"))
    lcov, python, output = [tmp_path / name for name in ("rust.lcov", "python.json", "report.json")]
    lcov.write_text(f"SF:{tmp_path / 'simulator/src/store.rs'}\nDA:1,1\nDA:2,{count}\nDA:3,1\n")
    python.write_text(
        json.dumps(
            {
                "files": {
                    "tools/validate_distributed_kv.py": {
                        "executed_lines": [1, 2],
                        "missing_lines": [],
                    }
                }
            }
        )
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "check_kv_coverage",
            "--base",
            "HEAD",
            "--rust-lcov",
            str(lcov),
            "--python-json",
            str(python),
            "--output",
            str(output),
        ],
    )
    if exit_code:
        with pytest.raises(SystemExit) as error:
            checker.main()
        assert error.value.code == exit_code
    else:
        checker.main()
    report = json.loads(output.read_text())
    assert report["passed"] is (exit_code == 0)
    assert report["totals"]["rust"]["covered"] == 2 + count
    assert "required 80%" in capsys.readouterr().out


def test_diff_uses_new_line_numbers_for_committed_edits(tmp_path):
    example_repo(tmp_path)
    subprocess.run(
        ["git", "add", "simulator/src/store.rs", "tools/validate_distributed_kv.py"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Coverage Test",
            "-c",
            "user.email=coverage@example.invalid",
            "commit",
            "-qm",
            "sources",
        ],
        cwd=tmp_path,
        check=True,
    )
    source = tmp_path / "tools/validate_distributed_kv.py"
    source.write_text("used = 8\nfree = 8 - used\nassert free == 0\n")
    assert checker.added_lines(tmp_path, "HEAD")["tools/validate_distributed_kv.py"] == {1, 3}


def test_each_new_source_file_must_pass_even_if_language_total_exceeds_eighty(tmp_path):
    example_repo(tmp_path)
    (tmp_path / "simulator/src/other.rs").write_text(
        "fn other() -> u64 {\n" + "    let value = 8;\n" * 10 + "    value\n}\n"
    )
    rust = {
        "simulator/src/store.rs": {1: 1, 2: 0, 3: 1},
        "simulator/src/other.rs": dict.fromkeys(range(1, 14), 1),
    }
    python = {
        "files": {
            "tools/validate_distributed_kv.py": {"executed_lines": [1, 2], "missing_lines": []}
        }
    }
    report = checker.build_report(tmp_path, "HEAD", rust, python, 80)
    assert report["totals"]["rust"]["percent"] == 100 * 15 / 16
    assert report["passed"] is False
    assert next(file for file in report["files"] if file["file"] == "simulator/src/store.rs")[
        "missing_lines"
    ] == [2]
