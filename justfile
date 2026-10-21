# VibeSim test-tier runner. One recipe per capability tier (see tests/conftest.py
# + skill `dev-run-tests`). Install just: `cargo install just` (or your package mgr).
#
#   just            # = test-cpu (the fast default gate)
#   just test-gpu   # GPU tier (throughput regression etc.)
#   just test-all   # cpu + gpu
#   just test-bench # separate perf step; run after test-all for full validation

# libpython dir — the Rust test binary embeds PyO3 and crashes on
# `libpython3.12.so` without it on LD_LIBRARY_PATH (uv pins the 3.12 interp).
libdir := `uv run --no-sync python -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"`

# DeepGEMM (grouped_gemm `deepgemm` backend) builds JIT-only and skips its
# git-clean version assert when this is 0; default is 1, which forces an
# install-time CUDA compile AND asserts a clean tree (fails in uv's build clone).
# Exported to every recipe so a cold `just sync` builds deep_gemm correctly
# without anyone passing it by hand. (Once built, uv caches the wheel by commit.)
export DG_USE_LOCAL_VERSION := "0"

# Default: the cpu gate.
default: test-cpu

# Sync the uv env (builds the pinned deep_gemm wheel on a cold cache). Use this
# as the install entrypoint so DG_USE_LOCAL_VERSION above is in effect.
sync:
    # no-build-isolation requires DeepGEMM's setup.py imports to be installed
    # before DeepGEMM itself. A single cold sync has no such install ordering.
    uv sync --inexact --no-install-package deep-gemm
    uv sync

# cpu tier — Rust unit tests + deterministic mocked pytest. No GPU/binary.
# `workers` is the xdist worker count. Not `auto`: that means one worker per core,
# and each worker pays ~9 s importing torch + flashinfer, so a big host spends
# more on startup than it saves. 16 is the measured knee here (17 s, against 19 s
# at 8 and 24); override on a smaller machine with `just test-cpu workers=4`.
test-cpu workers="16": sync
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib
    uv run --no-sync pytest -m "not gpu and not agent and not bench" -n {{workers}}

# gpu tier — needs a CUDA device. Auto-includes needs_binary/needs_db tests when
# present; the perf_api bridge / launcher set their own subprocess env.
test-gpu: sync
    uv run --no-sync pytest -m gpu

# agent tier — Codex runner+judge skill cases (expensive; opt-in).
test-agent: sync
    uv run --no-sync python tests/skill_tests/run_codex_skill_tests.py

# bench tier — separate perf step: sim-speed (warn-only) + Rust release microbenches (ignored).
test-bench: sync
    uv run --no-sync pytest -m bench
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --release -- --ignored --nocapture

# Main correctness gate (cpu + gpu). Full validation also runs `just test-bench`.
test-all: test-cpu test-gpu

# (Re)record per-GPU goldens for throughput + sim-speed on this device.
update-golden: sync
    uv run --no-sync pytest -m "gpu or bench" --update-golden

# CUDA/profile-independent protocol gate; uses an already prepared CPU env.
# Includes the real remote-admission always-miss shell equivalence test.
test-distributed-kv:
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib remote_cache
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib kv_store
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib worker::kv::full_attn
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib worker::kv::shared::prefix_cache
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib deployment::
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib pinned_mooncake
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib pinned_kvcached
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib log::kv_sampler
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo test -p simulator --lib sim::run
    uv run --no-sync python -m pytest tests/test_distributed_kv_validation.py tests/test_kv_storage_external_oracles.py tests/test_kv_storage_trace.py tests/test_kv_coverage.py -q

# Added executable lines versus the original KV base, Rust/Python independently.
# Requires cargo-llvm-cov, llvm-tools-preview, coverage>=7.10 and pinned checkouts.
test-distributed-kv-coverage base="48864532c291e5cd48d01efb515d63733496e420":
    test -n "${MOONCAKE_CHECKOUT:-}" && test -n "${KVCACHED_CHECKOUT:-}"
    mkdir -p target/kv-coverage-reports
    uv run --no-sync cargo llvm-cov clean --workspace
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo llvm-cov -p simulator --lib --no-report
    LD_LIBRARY_PATH="{{libdir}}:${LD_LIBRARY_PATH:-}" uv run --no-sync cargo llvm-cov -p analyzer --no-report
    uv run --no-sync cargo llvm-cov report -p simulator -p analyzer --lcov --output-path target/kv-coverage-reports/rust.lcov
    COVERAGE_FILE=target/kv-coverage-reports/python.coverage uv run --no-sync coverage erase --rcfile tools/kv_coverage.rc
    COVERAGE_FILE=target/kv-coverage-reports/python.coverage uv run --no-sync coverage run --rcfile tools/kv_coverage.rc -m pytest tests/test_l7_launcher.py tests/test_distributed_kv_validation.py tests/test_kv_storage_external_oracles.py tests/test_kv_storage_trace.py tests/test_kv_coverage.py -q
    COVERAGE_FILE=target/kv-coverage-reports/python.coverage uv run --no-sync coverage combine --rcfile tools/kv_coverage.rc
    COVERAGE_FILE=target/kv-coverage-reports/python.coverage uv run --no-sync coverage json --rcfile tools/kv_coverage.rc -o target/kv-coverage-reports/python.json
    uv run --no-sync python tools/check_kv_coverage.py --base {{base}} --rust-lcov target/kv-coverage-reports/rust.lcov --python-json target/kv-coverage-reports/python.json --output target/kv-coverage-reports/added-lines.json --minimum 80
